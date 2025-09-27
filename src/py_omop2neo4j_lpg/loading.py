# Copyright (c) 2025-2026 Gowtham Adamane Rao. All Rights Reserved.
#
# Licensed under the Prosperity Public License 3.0.0 (the "License").
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License in the LICENSE file at the root
# of this repository, or at: https://prosperitylicense.com/versions/3.0.0
#
# Commercial use beyond a 30-day trial requires a separate license.
from __future__ import annotations

import os
import pathlib
import urllib.parse
from glob import glob

import click
from neo4j import Driver, GraphDatabase

from .config import get_logger, settings

logger = get_logger(__name__)

# --- Neo4j Driver Setup ---


def get_driver() -> Driver:
    """Establishes connection with the Neo4j database and returns a driver object."""
    return GraphDatabase.driver(
        settings.NEO4J_URI, auth=(settings.NEO4J_USER, settings.NEO4J_PASSWORD)
    )


# --- Helper for running queries ---


def _execute_queries(driver: Driver, queries: list[str], ignore_errors: bool = False):
    """Helper function to execute a list of Cypher queries."""
    with driver.session() as session:
        for query in queries:
            try:
                logger.info("Executing: %s...", query[:120].strip())
                session.run(query)
            except Exception as e:
                logger.error("Failed to execute query: %s", query[:120].strip())
                logger.error(e)
                if not ignore_errors:
                    raise


# --- Database Cleanup ---


def clear_database(driver: Driver):
    """Drops all constraints and indexes, then deletes all nodes and relationships."""
    logger.info("Starting database clearing process.")
    with driver.session() as session:
        constraints = session.run("SHOW CONSTRAINTS YIELD name").data()
        indexes = session.run("SHOW INDEXES YIELD name").data()

    drop_constraints = [
        f"DROP CONSTRAINT {c['name']}" for c in constraints if c.get("name")
    ]
    drop_indexes = [f"DROP INDEX {i['name']}" for i in indexes if i.get("name")]

    if drop_constraints:
        logger.info("Dropping existing constraints...")
        _execute_queries(driver, drop_constraints, ignore_errors=True)
    if drop_indexes:
        logger.info("Dropping existing indexes...")
        _execute_queries(driver, drop_indexes, ignore_errors=True)

    logger.info("Deleting all nodes and relationships...")
    _execute_queries(driver, ["MATCH (n) DETACH DELETE n"])
    logger.info("Database cleared successfully.")


# --- Schema Setup ---


def create_constraints_and_indexes(driver: Driver):
    """Creates constraints and indexes as defined in the FRD."""
    logger.info("Creating constraints and indexes.")
    queries = [
        (
            "CREATE CONSTRAINT constraint_concept_id IF NOT EXISTS "
            "FOR (c:Concept) REQUIRE c.concept_id IS UNIQUE;"
        ),
        (
            "CREATE CONSTRAINT constraint_domain_id IF NOT EXISTS "
            "FOR (d:Domain) REQUIRE d.domain_id IS UNIQUE;"
        ),
        (
            "CREATE CONSTRAINT constraint_vocabulary_id IF NOT EXISTS "
            "FOR (v:Vocabulary) REQUIRE v.vocabulary_id IS UNIQUE;"
        ),
        (
            "CREATE INDEX index_concept_code IF NOT EXISTS "
            "FOR (c:Concept) ON (c.concept_code);"
        ),
        (
            "CREATE INDEX index_standard_label IF NOT EXISTS "
            "FOR (c:Standard) ON (c.concept_id);"
        ),
    ]
    _execute_queries(driver, queries)
    logger.info("Constraints and indexes created successfully.")


# --- Data Loading Orchestrator ---


def run_load_csv(batch_size: int | None = None):
    """
    Main orchestrator for the LOAD CSV method.
    Connects to Neo4j, clears the DB, sets up schema, and loads all data.
    """
    # --- Pre-flight check: Ensure CSV files exist ---
    export_dir = settings.EXPORT_DIR
    csv_files = glob(os.path.join(export_dir, "*.csv"))
    if not csv_files:
        error_msg = (
            f"No CSV files found in the export directory "
            f"'{os.path.abspath(export_dir)}'. "
            "Please run the 'extract' command first."
        )
        raise click.ClickException(error_msg)

    driver = get_driver()
    try:
        logger.info("Successfully connected to Neo4j.")

        clear_database(driver)
        create_constraints_and_indexes(driver)

        # Determine the batch size to use
        effective_batch_size = (
            batch_size if batch_size is not None else settings.LOAD_CSV_BATCH_SIZE
        )
        logger.info(
            "Starting data loading process with batch size: %s",
            effective_batch_size,
        )

        queries = get_loading_queries(effective_batch_size)
        _execute_queries(driver, queries)

        logger.info("All data loading tasks completed successfully.")

    except Exception as e:
        logger.error("An error occurred during the Neo4j loading process: %s", e)
        raise
    finally:
        driver.close()
        logger.info("Neo4j connection closed.")


def get_loading_queries(batch_size: int) -> list[str]:
    """Returns a list of all LOAD CSV queries."""

    def get_file_uri(filename: str) -> str:
        """
        Constructs a full, URL-encoded file URI for a given CSV filename,
        making it relative to the container's /import directory.
        """
        # Get the full path to the CSV file on the host
        host_csv_path = pathlib.Path(settings.EXPORT_DIR) / filename

        # A simple heuristic: find which known base dir is a parent of the csv path
        host_mount_point = pathlib.Path("export_test")
        if not str(host_csv_path).startswith("export_test"):
            host_mount_point = pathlib.Path("export")

        # Get the path relative to the mount point
        relative_path = host_csv_path.relative_to(host_mount_point)

        # URL-encode the relative path to handle spaces, etc.
        encoded_path = urllib.parse.quote(str(relative_path))

        return f"file:///{encoded_path}"

    load_domains = f"""
    LOAD CSV WITH HEADERS FROM '{get_file_uri('domain.csv')}' AS row
    CREATE (d:Domain {{
        domain_id: row.domain_id,
        name: row.domain_name,
        concept_id: toInteger(row.domain_concept_id)
    }});
    """

    load_vocabularies = f"""
    LOAD CSV WITH HEADERS FROM '{get_file_uri('vocabulary.csv')}' AS row
    CREATE (v:Vocabulary {{
        vocabulary_id: row.vocabulary_id,
        name: row.vocabulary_name,
        concept_id: toInteger(row.vocabulary_concept_id),
        vocabulary_reference: row.vocabulary_reference,
        vocabulary_version: row.vocabulary_version
    }});
    """

    load_concepts = f"""
    CALL {{
        LOAD CSV WITH HEADERS FROM '{get_file_uri('concepts_optimized.csv')}' AS row
        // 1. Create the Concept node with base properties
        CREATE (c:Concept {{
            concept_id: toInteger(row.concept_id),
            name: row.concept_name,
            domain_id: row.domain_id,
            vocabulary_id: row.vocabulary_id,
            concept_class_id: row.concept_class_id,
            standard_concept: row.standard_concept,
            concept_code: row.concept_code,
            valid_start_date: date(row.valid_start_date),
            valid_end_date: date(row.valid_end_date),
            invalid_reason: row.invalid_reason,
            synonyms: CASE
                WHEN row.synonyms IS NOT NULL THEN split(row.synonyms, '|')
                ELSE []
            END
        }})

        // 2. Add dynamic and conditional labels
        WITH c, row
        // Sanitize domain_id to create a valid label
        WITH c, row, apoc.text.upperCamelCase(
            apoc.text.regreplace(row.domain_id, '[^A-Za-z0-9]+', ' ')
        ) AS standardizedLabel
        CALL apoc.create.addLabels(c, [standardizedLabel]) YIELD node

        WITH c, row
        // Add :Standard label conditionally for optimized queries
        FOREACH (x IN CASE WHEN row.standard_concept = 'S' THEN [1] ELSE [] END |
            SET c:Standard
        )
        WITH c, row
        MATCH (d:Domain {{domain_id: row.domain_id}})
        CREATE (c)-[:IN_DOMAIN]->(d)
        WITH c, row
        MATCH (v:Vocabulary {{vocabulary_id: row.vocabulary_id}})
        CREATE (c)-[:FROM_VOCABULARY]->(v)
    }} IN TRANSACTIONS OF {batch_size} ROWS;
    """

    load_relationships = f"""
    CALL {{
        LOAD CSV WITH HEADERS FROM '{get_file_uri('concept_relationship.csv')}' AS row
        MATCH (c1:Concept {{concept_id: toInteger(row.concept_id_1)}})
        MATCH (c2:Concept {{concept_id: toInteger(row.concept_id_2)}})
        WITH c1, c2, row,
            toupper(
                apoc.text.replace(row.relationship_id, '[^A-Za-z0-9_]+', '_')
            ) AS relType
        CALL apoc.create.relationship(c1, relType, {{
            valid_start: date(row.valid_start_date),
            valid_end: date(row.valid_end_date),
            invalid_reason: row.invalid_reason
        }}, c2) YIELD rel
        RETURN count(rel) AS count
    }} IN TRANSACTIONS OF {batch_size} ROWS
    RETURN "relationships loaded"
    """

    load_ancestors = f"""
    CALL {{
        LOAD CSV WITH HEADERS FROM '{get_file_uri('concept_ancestor.csv')}' AS row
        MATCH (d:Concept {{concept_id: toInteger(row.descendant_concept_id)}})
        MATCH (a:Concept {{concept_id: toInteger(row.ancestor_concept_id)}})
        CREATE (d)-[r:HAS_ANCESTOR]->(a)
        SET r.min_levels = toInteger(row.min_levels_of_separation),
            r.max_levels = toInteger(row.max_levels_of_separation)
    }} IN TRANSACTIONS OF {batch_size} ROWS
    RETURN "ancestors loaded"
    """

    return [
        load_domains,
        load_vocabularies,
        load_concepts,
        load_relationships,
        load_ancestors,
    ]
