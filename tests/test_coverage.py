import unittest
from unittest.mock import patch
from click.testing import CliRunner
from click import ClickException
from py_omop2neo4j_lpg.cli import cli

class TestCoverage(unittest.TestCase):

    def setUp(self):
        self.runner = CliRunner()

    @patch("py_omop2neo4j_lpg.loading.get_driver")
    @patch("py_omop2neo4j_lpg.loading.clear_database", side_effect=ClickException("mock click exception"))
    def test_clear_db_click_exception(self, mock_clear, mock_driver):
        result = self.runner.invoke(cli, ["clear-db"])
        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("mock click exception", result.output)

    @patch("py_omop2neo4j_lpg.transformation.prepare_for_bulk_import", side_effect=ClickException("mock click exception"))
    def test_prepare_bulk_click_exception(self, mock_prepare):
        result = self.runner.invoke(cli, ["prepare-bulk"])
        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("mock click exception", result.output)

    @patch("py_omop2neo4j_lpg.loading.get_driver")
    @patch("py_omop2neo4j_lpg.loading.create_constraints_and_indexes", side_effect=ClickException("mock click exception"))
    def test_create_indexes_click_exception(self, mock_create, mock_driver):
        result = self.runner.invoke(cli, ["create-indexes"])
        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("mock click exception", result.output)

    @patch("py_omop2neo4j_lpg.validation.run_validation", side_effect=ClickException("mock click exception"))
    def test_validate_click_exception(self, mock_validate):
        result = self.runner.invoke(cli, ["validate"])
        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("mock click exception", result.output)