import os
import pytest
import psycopg2
from click.testing import CliRunner
import stat

from py_omop2neo4j_lpg.cli import cli
from py_omop2neo4j_lpg.config import settings


@pytest.mark.integration
def test_extract_with_non_writable_export_dir(pristine_db):
    """
    Tests that the extract command fails gracefully if the EXPORT_DIR is not writable.
    """
    runner = CliRunner()
    export_dir = settings.EXPORT_DIR

    # Make the directory read-only
    os.makedirs(export_dir, exist_ok=True)
    # Read-only for all
    read_only_perms = stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH
    os.chmod(export_dir, read_only_perms)

    try:
        result = runner.invoke(cli, ["extract"])
        assert result.exit_code != 0
        assert "is not writable" in result.output
    finally:
        # Restore write permissions to allow cleanup
        os.chmod(export_dir, stat.S_IWUSR | stat.S_IRUSR | stat.S_IXUSR | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH)


@pytest.mark.integration
def test_etl_with_orphan_relationship(pristine_db, neo4j_service):
    """
    Tests that the ETL process handles orphan relationships (a relationship
    pointing to a concept_id that does not exist) gracefully.
    The current implementation should simply not create the relationship.
    """
    orphan_concept_id = 9999  # This concept does not exist

    try:
        conn = psycopg2.connect(
            host="localhost", port=5433, user="testuser", password="testpass", dbname="testdb"
        )
        with conn.cursor() as cursor:
            # Insert a relationship pointing to a non-existent concept
            cursor.execute(
                """
                INSERT INTO concept_relationship (concept_id_1, concept_id_2, relationship_id, valid_start_date, valid_end_date, invalid_reason)
                VALUES (1, %s, 'Subsumes', '2000-01-01', '2099-12-31', NULL);
                """,
                (orphan_concept_id,)
            )
        conn.commit()
        conn.close()
    except Exception as e:
        pytest.fail(f"Failed to insert orphan relationship data: {e}")

    runner = CliRunner()
    assert runner.invoke(cli, ["extract"]).exit_code == 0
    assert runner.invoke(cli, ["load-csv"]).exit_code == 0

    # Verify that the relationship was not created in Neo4j
    from neo4j import GraphDatabase
    driver = GraphDatabase.driver("bolt://localhost:7688", auth=("neo4j", "StrongPass123"))
    with driver.session() as session:
        result = session.run("MATCH ()-[r:Subsumes]->() RETURN count(r) AS count")
        # There should be 0 'Subsumes' relationships, as the one we added was an orphan
        assert result.single()["count"] == 0
    driver.close()


@pytest.mark.integration
def test_cli_with_invalid_postgres_credentials(monkeypatch):
    """
    Tests that the CLI fails with a clear error message when PostgreSQL
    credentials are incorrect.
    """
    # Use monkeypatch to temporarily set invalid credentials
    monkeypatch.setattr(settings, 'POSTGRES_PASSWORD', 'wrongpassword')
    runner = CliRunner()
    result = runner.invoke(cli, ["extract"])
    assert result.exit_code != 0
    assert "Could not connect to PostgreSQL" in result.output
    assert "password authentication failed" in result.output


@pytest.mark.integration
def test_load_csv_with_missing_files(pristine_db, neo4j_service):
    """
    Tests that the `load-csv` command fails gracefully if the CSV files
    are not present in the export directory.
    """
    runner = CliRunner()
    # Ensure the export directory is empty
    export_dir = settings.EXPORT_DIR
    if os.path.exists(export_dir):
        for f in os.listdir(export_dir):
            os.remove(os.path.join(export_dir, f))

    result = runner.invoke(cli, ["load-csv"])
    assert result.exit_code != 0
    assert "No CSV files found" in result.output


@pytest.mark.integration
def test_prepare_bulk_with_empty_input_dir(pristine_db):
    """
    Tests that `prepare-bulk` handles an empty input directory correctly.
    """
    runner = CliRunner()
    # Ensure the export directory is empty
    export_dir = settings.EXPORT_DIR
    if os.path.exists(export_dir):
        for f in os.listdir(export_dir):
            os.remove(os.path.join(export_dir, f))
    else:
        os.makedirs(export_dir)

    result = runner.invoke(cli, ["prepare-bulk"])
    assert result.exit_code == 0
    assert "No files to process" in result.output


@pytest.mark.integration
def test_etl_with_windows_line_endings(pristine_db, neo4j_service):
    """
    Tests that the ETL process correctly handles data containing
    Windows-style line endings (CRLF, \\r\\n).
    """
    special_name = 'Concept with a\\r\\nWindows line ending'
    special_concept_id = 9998

    try:
        conn = psycopg2.connect(
            host="localhost", port=5433, user="testuser", password="testpass", dbname="testdb"
        )
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO concept (concept_id, concept_name, domain_id, vocabulary_id, concept_class_id, standard_concept, concept_code, valid_start_date, valid_end_date, invalid_reason)
                VALUES (%s, %s, 'Drug', 'RxNorm', 'Ingredient', 'S', 'WIN_EOL', '2000-01-01', '2099-12-31', NULL);
                """,
                (special_concept_id, special_name)
            )
        conn.commit()
        conn.close()
    except Exception as e:
        pytest.fail(f"Failed to insert special character data: {e}")

    runner = CliRunner()
    assert runner.invoke(cli, ["extract"]).exit_code == 0
    assert runner.invoke(cli, ["load-csv"]).exit_code == 0

    from neo4j import GraphDatabase
    driver = GraphDatabase.driver("bolt://localhost:7688", auth=("neo4j", "StrongPass123"))
    with driver.session() as session:
        result = session.run(
            "MATCH (c:Concept {concept_id: $id}) RETURN c.name AS name",
            id=special_concept_id
        )
        record = result.single()
        assert record is not None
        # The database should store the literal characters, not interpret them as a newline
        assert record["name"] == special_name
    driver.close()
