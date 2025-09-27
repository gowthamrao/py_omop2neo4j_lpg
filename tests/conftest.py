# Copyright (c) 2025-2026 Gowtham Adamane Rao. All Rights Reserved.
#
# Licensed under the Prosperity Public License 3.0.0 (the "License").
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License in the LICENSE file at the root
# of this repository, or at: https://prosperitylicense.com/versions/3.0.0
#
# Commercial use beyond a 30-day trial requires a separate license.

import os
import shutil
import time

import psycopg2
import pytest
from neo4j import GraphDatabase
from py_omop2neo4j_lpg.config import settings


@pytest.fixture(scope="session")
def test_export_dir():
    """Creates a directory for test exports."""
    export_dir = "./export_test"
    os.makedirs(export_dir, exist_ok=True)
    yield export_dir


@pytest.fixture(autouse=True)
def clean_export_dir(test_export_dir):
    """Cleans the export_test directory before each test."""
    export_dir = test_export_dir
    if os.path.exists(export_dir):
        for item in os.listdir(export_dir):
            item_path = os.path.join(export_dir, item)
            if os.path.isdir(item_path):
                shutil.rmtree(item_path)
            else:
                os.remove(item_path)


@pytest.fixture(autouse=True)
def monkeypatch_settings(monkeypatch, test_export_dir):
    """Monkeypatches the settings for the test environment."""
    monkeypatch.setattr(settings, "EXPORT_DIR", test_export_dir)


@pytest.fixture(scope="session")
def docker_compose_file(pytestconfig):
    """Returns the path to the docker-compose file for tests."""
    return os.path.join(str(pytestconfig.rootdir), "docker-compose.test.yml")


@pytest.fixture(scope="session")
def postgres_service(docker_services):
    """Waits for the PostgreSQL service to be healthy."""
    # Healthcheck for postgres
    deadline = time.time() + 60
    while time.time() < deadline:
        try:
            conn = psycopg2.connect(
                host="localhost",
                port=5433,
                user="testuser",
                password="testpass",
                dbname="testdb",
            )
            conn.close()
            break
        except psycopg2.OperationalError:
            time.sleep(1)
    else:
        pytest.fail("PostgreSQL did not become available in 60 seconds.")


@pytest.fixture(scope="session")
def neo4j_service(docker_services):
    """Waits for the Neo4j service to be healthy."""
    # Healthcheck for neo4j
    deadline = time.time() + 60
    while time.time() < deadline:
        try:
            driver = GraphDatabase.driver(
                "bolt://localhost:7688",
                auth=("neo4j", "StrongPass123"),
            )
            driver.verify_connectivity()
            driver.close()
            break
        except Exception:
            time.sleep(1)
    else:
        pytest.fail("Neo4j did not become available in 60 seconds.")


@pytest.fixture(scope="function")
def pristine_db(postgres_service):
    """
    Ensures the PostgreSQL database is in a clean, consistent state
    before each test function.
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
            # Drop all tables in the public schema
            cursor.execute("""
                DO $$ DECLARE
                    r RECORD;
                BEGIN
                    FOR r IN (
                        SELECT tablename FROM pg_tables
                        WHERE schemaname = 'public'
                    ) LOOP
                        EXECUTE
                            'DROP TABLE IF EXISTS '
                            || quote_ident(r.tablename)
                            || ' CASCADE';
                    END LOOP;
                END $$;
            """)
            # Re-initialize the database with sample data
            with open("./tests/sample_data/init.sql") as f:
                cursor.execute(f.read())
        conn.commit()
        conn.close()
    except Exception as e:
        pytest.fail(f"Failed to reset database: {e}")
    yield
