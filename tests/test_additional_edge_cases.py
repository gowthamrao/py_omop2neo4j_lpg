# Copyright (c) 2025-2026 Gowtham Adamane Rao. All Rights Reserved.
#
# Licensed under the Prosperity Public License 3.0.0 (the "License").
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License in the LICENSE file at the root
# of this repository, or at: https://prosperitylicense.com/versions/3.0.0
#
# Commercial use beyond a 30-day trial requires a separate license.

import tempfile
from unittest.mock import patch

import psycopg2
import pytest
from click.testing import CliRunner
from py_omop2neo4j_lpg.cli import cli
from py_omop2neo4j_lpg.config import settings


@pytest.mark.integration
def test_load_csv_idempotency(pristine_db, neo4j_service):
    """
    Tests that the `load-csv` command is idempotent, meaning running it
    multiple times produces the same state.
    """
    runner = CliRunner()

    # Initial ETL run
    assert runner.invoke(cli, ["extract"]).exit_code == 0
    assert runner.invoke(cli, ["load-csv"]).exit_code == 0

    # First validation
    result_validate1 = runner.invoke(cli, ["validate"])
    assert result_validate1.exit_code == 0
    validation_output1 = result_validate1.output

    # Run `load-csv` again
    result_load2 = runner.invoke(cli, ["load-csv"])
    assert result_load2.exit_code == 0, "The second `load-csv` run failed."

    # Second validation
    result_validate2 = runner.invoke(cli, ["validate"])
    assert result_validate2.exit_code == 0
    validation_output2 = result_validate2.output

    # Compare validation outputs
    assert (
        validation_output1 == validation_output2
    ), "Validation outputs differ after running `load-csv` a second time."


@pytest.mark.integration
def test_load_csv_with_duplicate_pk(pristine_db, neo4j_service):
    """
    Tests that the `load-csv` command fails gracefully when there is a
    primary key violation (duplicate concept_id).
    """
    try:
        conn = psycopg2.connect(
            host="localhost",
            port=5433,
            user="testuser",
            password="testpass",
            dbname="testdb",
        )
        with conn.cursor() as cursor:
            # Insert a concept, then insert another with the same concept_id
            # This simulates duplicate data in the source
            cursor.execute(
                """
                INSERT INTO concept (
                    concept_id, concept_name, domain_id, vocabulary_id,
                    concept_class_id, standard_concept, concept_code,
                    valid_start_date, valid_end_date
                ) VALUES (
                    1, 'Test Concept 1', 'Drug', 'RxNorm', 'Ingredient',
                    'S', 'CODE1', '2000-01-01', '2099-12-31'
                );
                """
            )
            cursor.execute(
                """
                INSERT INTO concept (
                    concept_id, concept_name, domain_id, vocabulary_id,
                    concept_class_id, standard_concept, concept_code,
                    valid_start_date, valid_end_date
                ) VALUES (
                    1, 'Duplicate Test Concept', 'Drug', 'RxNorm',
                    'Ingredient', 'S', 'CODE2', '2000-01-01', '2099-12-31'
                );
                """
            )
        conn.commit()
        conn.close()
    except Exception as e:
        pytest.fail(f"Failed to insert duplicate data for test setup: {e}")

    runner = CliRunner()
    assert runner.invoke(cli, ["extract"]).exit_code == 0

    # The load should fail because of the uniqueness constraint on :Concept(concept_id)
    result_load = runner.invoke(cli, ["load-csv"])
    assert result_load.exit_code != 0
    # Check for Neo4j's constraint violation error in the output
    assert "ConstraintValidationFailed" in result_load.output or (
        "already exists with label" in result_load.output
    )


@pytest.mark.unit
@patch("py_omop2neo4j_lpg.extraction.psycopg2.connect")
@patch(
    "py_omop2neo4j_lpg.extraction.open",
    side_effect=PermissionError("Permission denied"),
)
def test_extract_permission_error_on_write(mock_open, mock_connect):
    """
    Tests that `extract` fails gracefully with a PermissionError during file write,
    simulated by mocking `open`. This is a platform-independent unit test.
    """
    runner = CliRunner()
    with tempfile.TemporaryDirectory() as temp_dir:
        with patch.object(settings, "EXPORT_DIR", temp_dir):
            result = runner.invoke(cli, ["extract"])
            assert result.exit_code != 0
            assert "Failed to write" in result.output
            assert "Permission denied" in result.output


@pytest.mark.unit
@patch(
    "py_omop2neo4j_lpg.extraction.psycopg2.connect",
    side_effect=psycopg2.OperationalError(
        'password authentication failed for user "testuser"'
    ),
)
def test_with_invalid_db_credentials(mock_connect):
    """
    Tests that the application fails with a clear error if database
    credentials are incorrect by mocking the connection call.
    """
    runner = CliRunner()
    with tempfile.TemporaryDirectory() as temp_dir:
        with patch.object(settings, "EXPORT_DIR", temp_dir):
            result = runner.invoke(cli, ["extract"])
            assert result.exit_code != 0
            assert "password authentication failed" in result.output
