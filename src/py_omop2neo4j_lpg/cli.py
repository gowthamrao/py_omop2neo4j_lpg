# Copyright (c) 2025-2026 Gowtham Adamane Rao. All Rights Reserved.
#
# Licensed under the Prosperity Public License 3.0.0 (the "License").
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License in the LICENSE file at the root
# of this repository, or at: https://prosperitylicense.com/versions/3.0.0
#
# Commercial use beyond a 30-day trial requires a separate license.

"""
Command-Line Interface for the OMOP to Neo4j ETL tool.

This module provides a CLI using 'click' to orchestrate the different
stages of the ETL process, including extraction, loading, and database
management tasks.
"""

import json

import click

from . import extraction, loading, transformation, validation
from .config import get_logger, settings

logger = get_logger(__name__)


@click.group()
def cli():
    """A CLI tool to migrate OMOP vocabulary from PostgreSQL to Neo4j."""
    pass


@cli.command()
def extract():
    """
    Extracts OMOP vocabulary data from PostgreSQL to CSV files.
    """
    logger.info("CLI: Starting extraction process...")
    try:
        extraction.export_tables_to_csv()
        logger.info("CLI: Extraction process completed successfully.")
    except click.ClickException:
        # Re-raise exceptions from business logic that are already CLI-formatted
        raise
    except Exception as e:
        logger.error("CLI: An unexpected error occurred during extraction: %s", e)
        raise click.ClickException(f"An unexpected error occurred: {e}")


@cli.command()
def clear_db():
    """
    Clears the Neo4j database by deleting all nodes and relationships.
    Also drops all constraints and indexes.
    """
    logger.info("CLI: Starting database clearing process...")
    try:
        driver = loading.get_driver()
        loading.clear_database(driver)
        driver.close()
        logger.info("CLI: Database clearing process completed successfully.")
    except click.ClickException:
        raise
    except Exception as e:
        logger.error("CLI: An error occurred while clearing the database: %s", e)
        raise click.ClickException(f"An unexpected error occurred: {e}")


@cli.command()
@click.option(
    "--batch-size",
    default=None,
    type=int,
    help="Override LOAD_CSV_BATCH_SIZE from settings.",
)
def load_csv(batch_size):
    """
    Loads data from CSV files into Neo4j using the online LOAD CSV method.
    This is a full reload: it clears the DB, creates schema, and loads data.
    """
    logger.info("CLI: Starting LOAD CSV process...")
    try:
        loading.run_load_csv(batch_size=batch_size)
        logger.info("CLI: LOAD CSV process completed successfully.")
    except click.ClickException:
        raise
    except Exception as e:
        logger.error(
            "CLI: An unexpected error occurred during the LOAD CSV process: %s", e
        )
        raise click.ClickException(f"An unexpected error occurred: {e}")


@cli.command()
@click.option(
    "--chunk-size",
    default=settings.TRANSFORMATION_CHUNK_SIZE,
    show_default=True,
    type=int,
    help="Number of rows to process per chunk for large files.",
)
@click.option(
    "--import-dir",
    default="bulk_import",
    show_default=True,
    type=click.Path(),
    help="Directory to save formatted files for neo4j-admin import.",
)
def prepare_bulk(chunk_size, import_dir):
    """
    Prepares data files for the offline neo4j-admin import tool.
    This is the recommended method for very large datasets.
    """
    logger.info("CLI: Starting bulk import preparation process...")
    try:
        command = transformation.prepare_for_bulk_import(
            chunk_size=chunk_size, import_dir=import_dir
        )
        if command:
            logger.info("CLI: Bulk import preparation completed successfully.")
            click.secho("--- Neo4j-Admin Import Command ---", fg="green", bold=True)
            click.secho("1. Stop the Neo4j database service.", fg="yellow")
            click.secho("2. Run the following command from your terminal:", fg="yellow")
            click.echo(command)
            click.secho(
                "\n3. After the import is complete, start the Neo4j service.",
                fg="yellow",
            )
            click.secho(
                "4. Run `py_omop2neo4j_lpg create-indexes` to build the "
                "database schema.",
                fg="yellow",
            )

    except click.ClickException:
        raise
    except Exception as e:
        logger.error(
            "CLI: An unexpected error occurred during bulk import preparation: %s",
            e,
        )
        raise click.ClickException(f"An unexpected error occurred: {e}")


@cli.command()
def create_indexes():
    """
    Creates all predefined constraints and indexes in the Neo4j database.
    Useful after a manual import or if schema setup failed.
    """
    logger.info("CLI: Starting index and constraint creation process...")
    try:
        driver = loading.get_driver()
        loading.create_constraints_and_indexes(driver)
        driver.close()
        logger.info("CLI: Index and constraint creation completed successfully.")
    except click.ClickException:
        raise
    except Exception as e:
        logger.error("CLI: An error occurred during index/constraint creation: %s", e)
        raise click.ClickException(f"An unexpected error occurred: {e}")


@cli.command()
def validate():
    """
    Runs all validation checks and prints a JSON report.
    """
    logger.info("CLI: Starting validation process...")
    click.secho("--- Running Database Validation ---", fg="cyan", bold=True)
    try:
        results = validation.run_validation()
        if results.get("error"):
            click.secho(f"\nValidation failed: {results['error']}", fg="red")
        else:
            # Pretty-print the JSON results to the console
            click.secho("\nValidation complete. Results:", fg="green")
            click.echo(json.dumps(results, indent=2))

    except click.ClickException:
        raise
    except Exception as e:
        logger.error("CLI: A critical error occurred during validation: %s", e)
        raise click.ClickException(f"A critical error occurred during validation: {e}")


if __name__ == "__main__":  # pragma: no cover
    cli()
