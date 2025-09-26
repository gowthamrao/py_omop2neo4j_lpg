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
import stat
import shutil

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
def test_etl_with_spaces_in_path(pristine_db, neo4j_service, monkeypatch):
    """
    Tests that the ETL pipeline functions correctly when the EXPORT_DIR
    is a subdirectory with spaces in its path.
    """
    runner = CliRunner()
    # This path is inside the default `export_test` directory, which is mounted.
    spaced_export_dir = os.path.join(settings.EXPORT_DIR, "dir with spaces")

    # Ensure the directory is clean before running
    if os.path.exists(spaced_export_dir):
        shutil.rmtree(spaced_export_dir)
    os.makedirs(spaced_export_dir, exist_ok=True)

    # Temporarily change the EXPORT_DIR for this test
    monkeypatch.setattr(settings, 'EXPORT_DIR', spaced_export_dir)

    try:
        # Run the full ETL process
        result_extract = runner.invoke(cli, ["extract"])
        assert result_extract.exit_code == 0, result_extract.output

        # This will now fail until loading.py is fixed, which is the next step.
        result_load = runner.invoke(cli, ["load-csv"])
        assert result_load.exit_code == 0, result_load.output

        # A simple validation to ensure data was loaded
        from neo4j import GraphDatabase
        driver = GraphDatabase.driver("bolt://localhost:7688", auth=("neo4j", "StrongPass123"))
        with driver.session() as session:
            result = session.run("MATCH (n:Concept) RETURN count(n) AS count")
            assert result.single()["count"] > 0
        driver.close()

    finally:
        # Clean up the created subdirectory
        if os.path.exists(spaced_export_dir):
            shutil.rmtree(spaced_export_dir)


@pytest.mark.integration
def test_extract_with_readonly_export_dir(pristine_db, monkeypatch):
    """
    Tests that the extract command fails gracefully if the EXPORT_DIR is not writable.
    """
    runner = CliRunner()
    readonly_dir = "./readonly_export"
    # Ensure the directory is clean before running
    if os.path.exists(readonly_dir):
        # Ensure we have permissions to delete it
        os.chmod(readonly_dir, stat.S_IWUSR | stat.S_IRUSR | stat.S_IXUSR)
        shutil.rmtree(readonly_dir)

    os.makedirs(readonly_dir, exist_ok=True)
    # Make the directory read-only
    read_only_perms = stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH
    os.chmod(readonly_dir, read_only_perms)

    monkeypatch.setattr(settings, 'EXPORT_DIR', readonly_dir)

    try:
        result = runner.invoke(cli, ["extract"])
        assert result.exit_code != 0
        assert "PermissionError" in result.output or "is not writable" in result.output

    finally:
        # Restore write permissions to allow cleanup
        os.chmod(readonly_dir, stat.S_IWUSR | stat.S_IRUSR | stat.S_IXUSR)
        shutil.rmtree(readonly_dir)


@pytest.mark.integration
def test_load_csv_idempotency(pristine_db, neo4j_service):
    """
    Tests that the `load-csv` command is idempotent, meaning running it
    multiple times produces the same state as running it once.
    """
    runner = CliRunner()

    # Initial ETL run
    assert runner.invoke(cli, ["extract"]).exit_code == 0
    assert runner.invoke(cli, ["load-csv"]).exit_code == 0

    # Get the state of the database after the first run
    from neo4j import GraphDatabase
    driver = GraphDatabase.driver("bolt://localhost:7688", auth=("neo4j", "StrongPass123"))
    with driver.session() as session:
        result1 = session.run("MATCH (n) RETURN labels(n) as labels, count(n) as count ORDER BY labels").data()
        rel_result1 = session.run("MATCH ()-[r]->() RETURN type(r) as type, count(r) as count ORDER BY type").data()

    # Re-run the load command
    assert runner.invoke(cli, ["load-csv"]).exit_code == 0

    # Get the state of the database after the second run
    with driver.session() as session:
        result2 = session.run("MATCH (n) RETURN labels(n) as labels, count(n) as count ORDER BY labels").data()
        rel_result2 = session.run("MATCH ()-[r]->() RETURN type(r) as type, count(r) as count ORDER BY type").data()

    driver.close()

    # The node counts and relationship counts should be identical
    assert result1 == result2
    assert rel_result1 == rel_result2


@pytest.mark.integration
def test_etl_with_null_concept_name(pristine_db, neo4j_service):
    """
    Tests that the ETL process can handle a NULL value in the `concept_name`
    field, which is typically expected to be NOT NULL. The node should still
    be created, but the `name` property should be absent or null.
    """
    null_name_concept_id = 10000
    conn = None

    try:
        conn = psycopg2.connect(
            host="localhost", port=5433, user="testuser", password="testpass", dbname="testdb"
        )
        with conn.cursor() as cursor:
            # Temporarily remove the NOT NULL constraint for this test
            cursor.execute("ALTER TABLE concept ALTER COLUMN concept_name DROP NOT NULL;")

            # Add a concept with a NULL name
            cursor.execute(
                """
                INSERT INTO concept (concept_id, concept_name, domain_id, vocabulary_id, concept_class_id, standard_concept, concept_code, valid_start_date, valid_end_date, invalid_reason)
                VALUES (%s, NULL, 'Drug', 'RxNorm', 'Ingredient', 'S', 'NULL_NAME', '2000-01-01', '2099-12-31', NULL);
                """,
                (null_name_concept_id,)
            )
        conn.commit()

        runner = CliRunner()
        assert runner.invoke(cli, ["extract"]).exit_code == 0
        assert runner.invoke(cli, ["load-csv"]).exit_code == 0

        # Verify the data in Neo4j
        from neo4j import GraphDatabase
        driver = GraphDatabase.driver("bolt://localhost:7688", auth=("neo4j", "StrongPass123"))
        with driver.session() as session:
            result = session.run(
                "MATCH (c:Concept {concept_id: $id}) RETURN c.name AS name, c.concept_name as concept_name",
                id=null_name_concept_id
            )
            record = result.single()
            assert record is not None
            # The property should be missing or null after import
            assert record.get("name") is None
            assert record.get("concept_name") is None
        driver.close()

    finally:
        # Ensure the constraint is restored and data is cleaned up
        if conn:
            with conn.cursor() as cursor:
                # Delete the test data before restoring the constraint
                cursor.execute("DELETE FROM concept WHERE concept_id = %s;", (null_name_concept_id,))
                # Restore the NOT NULL constraint
                cursor.execute("ALTER TABLE concept ALTER COLUMN concept_name SET NOT NULL;")
            conn.commit()
            conn.close()