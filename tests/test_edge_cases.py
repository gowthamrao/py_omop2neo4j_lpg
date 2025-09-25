# Copyright (c) 2025-2026 Gowtham Adamane Rao. All Rights Reserved.
#
# Licensed under the Prosperity Public License 3.0.0 (the "License").
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License in the LICENSE file at the root
# of this repository, or at: https://prosperitylicense.com/versions/3.0.0
#
# Commercial use beyond a 30-day trial requires a separate license.

import os
import pytest
import psycopg2
from click.testing import CliRunner
from unittest.mock import patch
from neo4j.exceptions import ServiceUnavailable

from py_omop2neo4j_lpg.cli import cli
from py_omop2neo4j_lpg.config import settings


TABLES_TO_TRUNCATE = [
    "concept",
    "vocabulary",
    "domain",
    "concept_class",
    "concept_relationship",
    "relationship",
    "concept_synonym",
    "concept_ancestor",
]


@pytest.mark.integration
def test_etl_with_empty_tables(pristine_db, neo4j_service):
    """
    Tests that the full ETL pipeline runs successfully when the source
    PostgreSQL tables are present but empty.
    """
    # Truncate all tables to create the "empty tables" condition
    try:
        conn = psycopg2.connect(
            host="localhost",
            port=5433,
            user="testuser",
            password="testpass",
            dbname="testdb"
        )
        with conn.cursor() as cursor:
            for table in TABLES_TO_TRUNCATE:
                cursor.execute(f"TRUNCATE TABLE {table} RESTART IDENTITY;")
        conn.commit()
        conn.close()
    except Exception as e:
        pytest.fail(f"Failed to truncate tables: {e}")

    runner = CliRunner()

    # 1. Extract - should produce empty or header-only files
    result_extract = runner.invoke(cli, ["extract"])
    assert result_extract.exit_code == 0, result_extract.output
    # Check that a representative file exists (it should have a header)
    assert os.path.exists(os.path.join(settings.EXPORT_DIR, "concepts_optimized.csv"))

    # 2. Load CSV - should run without error
    result_load = runner.invoke(cli, ["load-csv"])
    assert result_load.exit_code == 0, result_load.output

    # 3. Validate - should show an empty database
    result_validate = runner.invoke(cli, ["validate"])
    assert result_validate.exit_code == 0, result_validate.output
    assert '"node_counts_by_label_combination": {}' in result_validate.output
    assert '"relationship_counts_by_type": {}' in result_validate.output


@pytest.mark.integration
def test_extract_with_missing_table(pristine_db):
    """
    Tests that the extract command fails gracefully if a required
    table is missing from the source database.
    """
    try:
        conn = psycopg2.connect(
            host="localhost",
            port=5433,
            user="testuser",
            password="testpass",
            dbname="testdb"
        )
        with conn.cursor() as cursor:
            # Drop a required table
            cursor.execute("DROP TABLE concept CASCADE;")
        conn.commit()
        conn.close()
    except Exception as e:
        pytest.fail(f"Failed to drop table for test setup: {e}")

    runner = CliRunner()
    result = runner.invoke(cli, ["extract"])

    # Expect a non-zero exit code indicating failure
    assert result.exit_code != 0
    # Expect a clear error message from ClickException
    assert "Error:" in result.output
    assert "relation \"public.concept\" does not exist" in result.output


@pytest.mark.integration
def test_etl_with_special_characters(pristine_db, neo4j_service):
    """
    Tests that the ETL process correctly handles data containing special
    characters like commas, quotes, and newlines.
    """
    special_name = 'Concept, with "quotes" and a\nnew line'
    special_concept_id = 9999

    try:
        conn = psycopg2.connect(
            host="localhost",
            port=5433,
            user="testuser",
            password="testpass",
            dbname="testdb"
        )
        with conn.cursor() as cursor:
            # Add a concept with special characters in its name
            cursor.execute(
                """
                INSERT INTO concept (concept_id, concept_name, domain_id, vocabulary_id, concept_class_id, standard_concept, concept_code, valid_start_date, valid_end_date, invalid_reason)
                VALUES (%s, %s, 'Drug', 'RxNorm', 'Ingredient', 'S', 'SPL_CHAR', '2000-01-01', '2099-12-31', NULL);
                """,
                (special_concept_id, special_name)
            )
        conn.commit()
        conn.close()
    except Exception as e:
        pytest.fail(f"Failed to insert special character data: {e}")

    runner = CliRunner()
    # Run the full ETL
    assert runner.invoke(cli, ["extract"]).exit_code == 0
    assert runner.invoke(cli, ["load-csv"]).exit_code == 0

    # Verify the data in Neo4j
    from neo4j import GraphDatabase
    driver = GraphDatabase.driver("bolt://localhost:7688", auth=("neo4j", "StrongPass123"))
    with driver.session() as session:
        result = session.run(
            "MATCH (c:Concept {concept_id: $id}) RETURN c.name AS name",
            id=special_concept_id
        )
        record = result.single()
        assert record is not None
        assert record["name"] == special_name
    driver.close()


@pytest.mark.unit
@patch("py_omop2neo4j_lpg.loading.glob")
@patch("py_omop2neo4j_lpg.loading.get_driver")
@patch("py_omop2neo4j_lpg.loading._execute_queries", side_effect=Exception("Malformed CSV"))
def test_load_csv_command_malformed_csv(mock_execute, mock_get_driver, mock_glob):
    # Test case where the CSV file is malformed
    runner = CliRunner()
    mock_glob.return_value = ['dummy.csv']
    result = runner.invoke(cli, ["load-csv"])
    assert result.exit_code != 0
    assert "An unexpected error occurred: Malformed CSV" in result.output


@pytest.mark.unit
@patch("py_omop2neo4j_lpg.loading.glob")
@patch("py_omop2neo4j_lpg.loading.get_driver", side_effect=ServiceUnavailable("Connection error"))
def test_load_csv_command_neo4j_unavailable(mock_get_driver, mock_glob):
    # Test case where the Neo4j instance is unavailable
    runner = CliRunner()
    mock_glob.return_value = ['dummy.csv']
    result = runner.invoke(cli, ["load-csv"])
    assert result.exit_code != 0
    assert "An unexpected error occurred: Connection error" in result.output
