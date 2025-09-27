# Copyright (c) 2025-2026 Gowtham Adamane Rao. All Rights Reserved.
#
# Licensed under the Prosperity Public License 3.0.0 (the "License").
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License in the LICENSE file at the root
# of this repository, or at: https://prosperitylicense.com/versions/3.0.0
#
# Commercial use beyond a 30-day trial requires a separate license.

import unittest
from unittest.mock import patch

from click.testing import CliRunner
from py_omop2neo4j_lpg.cli import cli


class TestCli(unittest.TestCase):
    def setUp(self):
        self.runner = CliRunner()

    @patch("py_omop2neo4j_lpg.extraction.export_tables_to_csv")
    def test_extract_command(self, mock_export):
        result = self.runner.invoke(cli, ["extract"])
        self.assertEqual(result.exit_code, 0)
        mock_export.assert_called_once()

    @patch("py_omop2neo4j_lpg.loading.clear_database")
    @patch("py_omop2neo4j_lpg.loading.get_driver")
    def test_clear_db_command(self, mock_get_driver, mock_clear):
        result = self.runner.invoke(cli, ["clear-db"])
        self.assertEqual(result.exit_code, 0)
        mock_get_driver.assert_called_once()
        mock_clear.assert_called_once()

    @patch("py_omop2neo4j_lpg.loading.run_load_csv")
    def test_load_csv_command(self, mock_run_load):
        # Test without option
        result = self.runner.invoke(cli, ["load-csv"])
        self.assertEqual(result.exit_code, 0)
        mock_run_load.assert_called_with(batch_size=None)

        # Test with option
        result = self.runner.invoke(cli, ["load-csv", "--batch-size", "5000"])
        self.assertEqual(result.exit_code, 0)
        mock_run_load.assert_called_with(batch_size=5000)

    @patch("py_omop2neo4j_lpg.transformation.prepare_for_bulk_import")
    def test_prepare_bulk_command(self, mock_prepare_bulk):
        mock_prepare_bulk.return_value = "neo4j-admin command"
        result = self.runner.invoke(cli, ["prepare-bulk", "--chunk-size", "50000"])
        self.assertEqual(result.exit_code, 0)
        self.assertIn("neo4j-admin command", result.output)
        mock_prepare_bulk.assert_called_with(chunk_size=50000, import_dir="bulk_import")

    @patch("py_omop2neo4j_lpg.loading.create_constraints_and_indexes")
    @patch("py_omop2neo4j_lpg.loading.get_driver")
    def test_create_indexes_command(self, mock_get_driver, mock_create_indexes):
        result = self.runner.invoke(cli, ["create-indexes"])
        self.assertEqual(result.exit_code, 0)
        mock_get_driver.assert_called_once()
        mock_create_indexes.assert_called_once()

    @patch("py_omop2neo4j_lpg.validation.run_validation")
    def test_validate_command(self, mock_run_validation):
        # Mock the return value of the main validation orchestrator
        mock_run_validation.return_value = {
            "node_counts_by_label_combination": {
                "Concept:Standard": 100,
                "Domain": 5,
            },
            "sample_concept_verification": {"name": "Test Concept"},
        }

        # Invoke the command without the --concept-id option
        result = self.runner.invoke(cli, ["validate"])

        # Check that the command executed successfully
        self.assertEqual(result.exit_code, 0)

        # Check that our mock was called
        mock_run_validation.assert_called_once()

        # Check that the output contains key parts of the JSON report
        self.assertIn('"Concept:Standard": 100', result.output)
        self.assertIn('"Domain": 5', result.output)
        self.assertIn('"name": "Test Concept"', result.output)


if __name__ == "__main__":
    unittest.main()


class TestCliFailures(unittest.TestCase):
    def setUp(self):
        self.runner = CliRunner()

    @patch(
        "py_omop2neo4j_lpg.extraction.export_tables_to_csv",
        side_effect=Exception("mock error"),
    )
    def test_extract_failure(self, mock_export):
        result = self.runner.invoke(cli, ["extract"])
        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("mock error", result.output)

    @patch(
        "py_omop2neo4j_lpg.loading.get_driver",
        side_effect=Exception("mock error"),
    )
    def test_clear_db_failure(self, mock_get_driver):
        result = self.runner.invoke(cli, ["clear-db"])
        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("mock error", result.output)

    @patch(
        "py_omop2neo4j_lpg.loading.run_load_csv",
        side_effect=Exception("mock error"),
    )
    def test_load_csv_failure(self, mock_run_load):
        result = self.runner.invoke(cli, ["load-csv"])
        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("mock error", result.output)

    @patch(
        "py_omop2neo4j_lpg.transformation.prepare_for_bulk_import",
        side_effect=Exception("mock error"),
    )
    def test_prepare_bulk_failure(self, mock_prepare_bulk):
        result = self.runner.invoke(cli, ["prepare-bulk"])
        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("mock error", result.output)

    @patch(
        "py_omop2neo4j_lpg.loading.get_driver",
        side_effect=Exception("mock error"),
    )
    def test_create_indexes_failure(self, mock_get_driver):
        result = self.runner.invoke(cli, ["create-indexes"])
        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("mock error", result.output)

    @patch(
        "py_omop2neo4j_lpg.validation.run_validation",
        side_effect=Exception("mock error"),
    )
    def test_validate_exception_failure(self, mock_run_validation):
        result = self.runner.invoke(cli, ["validate"])
        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("mock error", result.output)

    @patch(
        "py_omop2neo4j_lpg.validation.run_validation",
        return_value={"error": "a validation error"},
    )
    def test_validate_soft_failure(self, mock_run_validation):
        result = self.runner.invoke(cli, ["validate"])
        self.assertEqual(result.exit_code, 0)
        self.assertIn("a validation error", result.output)
