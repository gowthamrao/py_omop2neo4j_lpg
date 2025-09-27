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
import tempfile

import pytest
from click.testing import CliRunner
from py_omop2neo4j_lpg.cli import cli
from py_omop2neo4j_lpg.config import settings


@pytest.mark.integration
def test_full_etl_pipeline(pristine_db, neo4j_service, docker_services):
    try:
        runner = CliRunner()

        # 1. Extract
        result_extract = runner.invoke(cli, ["extract"])
        assert result_extract.exit_code == 0
        assert os.path.exists(
            os.path.join(settings.EXPORT_DIR, "concepts_optimized.csv")
        )

        # 2. Load CSV
        result_load = runner.invoke(cli, ["load-csv"])
        assert result_load.exit_code == 0

        # 3. Validate
        result_validate = runner.invoke(cli, ["validate"])
        assert result_validate.exit_code == 0
        assert '"Concept:Drug:Standard": 1' in result_validate.output
        assert '"Concept:Condition:Standard": 1' in result_validate.output
        assert '"Domain": 2' in result_validate.output
        assert '"Vocabulary": 2' in result_validate.output
        assert '"TREATS": 1' in result_validate.output
        assert '"MAPS_TO": 1' in result_validate.output
        assert '"HAS_ANCESTOR": 1' in result_validate.output

    finally:
        # Print logs if the test fails
        logs = docker_services._docker_compose.execute("logs postgres-test")
        print(logs)


@pytest.mark.integration
def test_prepare_bulk_workflow(pristine_db, neo4j_service, docker_services):
    runner = CliRunner()
    # Use a temporary directory for the bulk import files
    bulk_import_dir = tempfile.mkdtemp()

    try:
        # 1. Extract
        result_extract = runner.invoke(cli, ["extract"])
        assert result_extract.exit_code == 0
        assert os.path.exists(
            os.path.join(settings.EXPORT_DIR, "concepts_optimized.csv")
        )

        # 2. Prepare for bulk import
        result_prepare = runner.invoke(
            cli, ["prepare-bulk", "--import-dir", bulk_import_dir]
        )
        assert result_prepare.exit_code == 0

        # 3. Verify file creation
        assert os.path.exists(os.path.join(bulk_import_dir, "nodes_concept.csv"))
        assert os.path.exists(os.path.join(bulk_import_dir, "rels_semantic.csv"))

        # 4. Verify command output
        assert "neo4j-admin database import full" in result_prepare.output
        assert "--nodes='nodes_concept.csv'" in result_prepare.output
        assert "--relationships='rels_semantic.csv'" in result_prepare.output

    finally:
        # Clean up created files
        shutil.rmtree(bulk_import_dir)
