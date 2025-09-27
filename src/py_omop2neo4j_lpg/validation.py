# Copyright (c) 2025-2026 Gowtham Adamane Rao. All Rights Reserved.
#
# Licensed under the Prosperity Public License 3.0.0 (the "License").
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License in the LICENSE file at the root
# of this repository, or at: https://prosperitylicense.com/versions/3.0.0
#
# Commercial use beyond a 30-day trial requires a separate license.

from __future__ import annotations

import json
from typing import Any

from neo4j import Driver

from .config import get_logger
from .loading import get_driver

logger = get_logger(__name__)


def get_node_counts(driver: Driver) -> dict[str, int]:
    """
    Counts nodes for each distinct combination of labels in the database.
    This provides a detailed breakdown, e.g., for 'Concept:Drug:Standard'.
    """
    logger.info("Performing node count validation by label combination...")
    query = """
    MATCH (n)
    WITH labels(n) AS label_combination
    RETURN label_combination, count(*) AS count
    ORDER BY count DESC
    """
    with driver.session() as session:
        result = session.run(query)
        # Sort labels within each combination for consistent keys
        counts = {
            ":".join(sorted(record["label_combination"])): record["count"]
            for record in result
            if record["label_combination"]
        }
        log_output = json.dumps(counts, indent=2)
        logger.info("Node counts by label combination: \n%s", log_output)
        return counts


def get_relationship_counts(driver: Driver) -> dict[str, int]:
    """
    Counts relationships for each distinct type in the database.
    """
    logger.info("Performing relationship count validation by type...")
    query = """
    CALL db.relationshipTypes() YIELD relationshipType
    CALL apoc.cypher.run(
        'MATCH ()-[:`' + relationshipType + '`]->() RETURN count(*) as count', {}
    ) YIELD value
    RETURN relationshipType, value.count AS count
    ORDER BY relationshipType
    """
    with driver.session() as session:
        result = session.run(query)
        counts = {record["relationshipType"]: record["count"] for record in result}
        log_output = json.dumps(counts, indent=2)
        logger.info("Relationship counts: \n%s", log_output)
        return counts


def verify_sample_concept(
    driver: Driver, concept_id: int = 1177480
) -> dict[str, Any] | None:
    """
    Fetches a sample concept to verify its structure.
    The default concept_id is 1177480 ('Enalapril').
    """
    logger.info("Performing structural validation for Concept ID: %s...", concept_id)
    query = """
    MATCH (c:Concept {concept_id: $concept_id})
    CALL {
        WITH c MATCH (c)-[r]->(neighbor)
        RETURN type(r) AS rel_type,
               collect({
                   name: neighbor.name,
                   id: COALESCE(
                       neighbor.concept_id,
                       neighbor.domain_id,
                       neighbor.vocabulary_id
                   )
               }) AS neighbors
    }
    WITH c, collect({rel_type: rel_type, neighbors: neighbors}) AS relationships
    WITH c, [rel IN relationships WHERE rel.rel_type IS NOT NULL] AS relationships
    OPTIONAL MATCH (ancestor:Concept)-[:HAS_ANCESTOR]->(c)
    WITH c, relationships,
         collect(
             CASE
                 WHEN ancestor IS NOT NULL
                 THEN {name: ancestor.name, id: ancestor.concept_id}
                 ELSE null
             END
         ) as ancestors
    RETURN
        c.concept_id AS concept_id,
        c.name AS name,
        labels(c) AS labels,
        size(c.synonyms) AS synonym_count,
        relationships,
        [ancestor IN ancestors WHERE ancestor IS NOT NULL] as ancestors
    """
    with driver.session() as session:
        result = session.run(query, concept_id=concept_id).single()
        if not result or not result.get("concept_id"):
            logger.warning("Sample Concept ID %s not found in database.", concept_id)
            return None

        record_dict: dict[str, Any] = result.data()

        # Clean up for better logging
        rels_summary = {}
        for item in record_dict.pop("relationships", []):
            if item.get("rel_type"):
                rels_summary[item["rel_type"]] = {
                    "count": len(item["neighbors"]),
                    "sample_neighbors": [n["name"] for n in item["neighbors"][:3]],
                }
        record_dict["relationships_summary"] = rels_summary

        ancestors_list = record_dict.pop("ancestors", [])
        record_dict["ancestors_summary"] = {
            "count": len(ancestors_list),
            "sample_ancestors": [a["name"] for a in ancestors_list[:5]],
        }

        if "labels" in record_dict and record_dict.get("labels"):
            record_dict["labels"] = sorted(record_dict["labels"])

        log_output = json.dumps(record_dict, indent=2)
        logger.info(
            "Structural validation for '%s': \n%s",
            record_dict.get("name"),
            log_output,
        )
        return record_dict


def run_validation() -> dict[str, Any]:
    """
    Main orchestrator for the validation process.
    Connects to Neo4j and runs all validation checks.
    """
    logger.info("Starting validation process...")
    driver = None
    try:
        driver = get_driver()
        node_counts = get_node_counts(driver)
        rel_counts = get_relationship_counts(driver)
        sample_verification = verify_sample_concept(driver)

        return {
            "node_counts_by_label_combination": node_counts,
            "relationship_counts_by_type": rel_counts,
            "sample_concept_verification": sample_verification,
        }

    except Exception as e:
        logger.error("An error occurred during the validation process: %s", e)
        return {"error": str(e)}
    finally:
        if driver:
            driver.close()
            logger.info("Validation process finished. Neo4j connection closed.")
