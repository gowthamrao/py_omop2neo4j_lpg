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
from click.testing import CliRunner

from py_omop2neo4j_lpg.cli import cli
from py_omop2neo4j_lpg.config import settings


@pytest.fixture(scope="session")
def docker_compose_file(pytestconfig):
    return os.path.join(
        str(pytestconfig.rootdir), "docker-compose.test.yml"
    )


import psycopg2
import time

@pytest.fixture(scope="session")
def postgres_service(docker_services):
    # Healthcheck for postgres
    deadline = time.time() + 60
    while time.time() < deadline:
        try:
            conn = psycopg2.connect(
                host="localhost",
                port=5433,
                user="testuser",
                password="testpass",
                dbname="testdb"
            )
            conn.close()
            break
        except psycopg2.OperationalError:
            time.sleep(1)
    else:
        pytest.fail("PostgreSQL did not become available in 60 seconds.")


from neo4j import GraphDatabase

@pytest.fixture(scope="session")
def neo4j_service(docker_services):
    # Healthcheck for neo4j
    deadline = time.time() + 60
    while time.time() < deadline:
        try:
            driver = GraphDatabase.driver("bolt://localhost:7688", auth=("neo4j", "StrongPass123"))
            driver.verify_connectivity()
            driver.close()
            break
        except Exception:
            time.sleep(1)
    else:
        pytest.fail("Neo4j did not become available in 60 seconds.")


import pytest

@pytest.mark.integration
def test_etl_idempotency_and_clear(pristine_db, neo4j_service, docker_services):
    try:
        runner = CliRunner()

        # 1. Extract
        result_extract = runner.invoke(cli, ["extract"])
        assert result_extract.exit_code == 0
        assert os.path.exists(os.path.join(settings.EXPORT_DIR, "concepts_optimized.csv"))

        #
        #
        # 2. Load CSV - First run
        result_load_1 = runner.invoke(cli, ["load-csv"])
        assert result_load_1.exit_code == 0

        # 3. Validate - First run
        result_validate_1 = runner.invoke(cli, ["validate"])
        assert result_validate_1.exit_code == 0
        assert '"Concept:Drug:Standard": 1' in result_validate_1.output

        # 4. Load CSV - Second run (idempotency check)
        result_load_2 = runner.invoke(cli, ["load-csv"])
        assert result_load_2.exit_code == 0

        # 5. Validate - Second run
        result_validate_2 = runner.invoke(cli, ["validate"])
        assert result_validate_2.exit_code == 0
        assert '"Concept:Drug:Standard": 1' in result_validate_2.output

        # 6. Clear the database
        result_clear = runner.invoke(cli, ["clear-db"])
        assert result_clear.exit_code == 0

        # 7. Validate - After clear
        result_validate_3 = runner.invoke(cli, ["validate"])
        assert result_validate_3.exit_code == 0
        assert '"node_counts_by_label_combination": {}' in result_validate_3.output
        assert '"relationship_counts_by_type": {}' in result_validate_3.output


    finally:
        # Clean up is handled by the clean_export_dir fixture
        pass
