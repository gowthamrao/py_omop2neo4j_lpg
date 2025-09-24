# Copyright (c) 2025-2026 Gowtham Adamane Rao. All Rights Reserved.
#
# Licensed under the Prosperity Public License 3.0.0 (the "License").
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License in the LICENSE file at the root
# of this repository, or at: https://prosperitylicense.com/versions/3.0.0
#
# Commercial use beyond a 30-day trial requires a separate license.

import unittest
from unittest.mock import MagicMock
from py_omop2neo4j_lpg import validation


class TestValidation(unittest.TestCase):

    def test_get_node_counts_new_format(self):
        # Arrange
        mock_driver = MagicMock()
        mock_session = MagicMock()
        mock_result = MagicMock()

        # Simulate the new result format from the updated Cypher query
        mock_records = [
            {"label_combination": ["Concept", "Drug", "Standard"], "count": 500},
            {"label_combination": ["Concept", "Drug"], "count": 1000},
            {"label_combination": ["Concept"], "count": 1500},
            {"label_combination": ["Domain"], "count": 10},
        ]
        mock_result.__iter__.return_value = iter(mock_records)
        mock_session.run.return_value = mock_result
        mock_driver.session.return_value.__enter__.return_value = mock_session

        # Act
        counts = validation.get_node_counts(mock_driver)

        # Assert
        self.assertEqual(len(counts), 4)
        # Check that labels are sorted and joined correctly
        self.assertEqual(counts["Concept:Drug:Standard"], 500)
        self.assertEqual(counts["Concept:Drug"], 1000)
        self.assertEqual(counts["Domain"], 10)
        self.assertTrue(mock_session.run.called)

    def test_get_relationship_counts(self):
        # Arrange
        mock_driver = MagicMock()
        mock_session = MagicMock()
        mock_result = MagicMock()

        mock_records = [
            {"relationshipType": "IS_A", "count": 2000},
            {"relationshipType": "HAS_ANCESTOR", "count": 50000},
        ]
        mock_result.__iter__.return_value = iter(mock_records)
        mock_session.run.return_value = mock_result
        mock_driver.session.return_value.__enter__.return_value = mock_session

        # Act
        counts = validation.get_relationship_counts(mock_driver)

        # Assert
        self.assertEqual(len(counts), 2)
        self.assertEqual(counts["HAS_ANCESTOR"], 50000)
        self.assertTrue(mock_session.run.called)

    def test_verify_sample_concept_with_ancestors(self):
        # Arrange
        mock_driver = MagicMock()
        mock_session = MagicMock()

        mock_record_data = {
            "concept_id": 1177480,
            "name": "Enalapril",
            "labels": ["Standard", "Drug", "Concept"],
            "synonym_count": 5,
            "relationships": [
                {
                    "rel_type": "IS_A",
                    "neighbors": [{"name": "ACE Inhibitor", "id": 123}],
                }
            ],
            "ancestors": [{"name": "Cardiovascular Agent", "id": 456}],
        }

        # This mock needs to behave like a neo4j Record object,
        # which has a .data() method and supports .get()
        mock_single_result = MagicMock()
        mock_single_result.data.return_value = mock_record_data
        mock_single_result.get.side_effect = mock_record_data.get

        mock_session.run.return_value.single.return_value = mock_single_result
        mock_driver.session.return_value.__enter__.return_value = mock_session

        # Act
        data = validation.verify_sample_concept(mock_driver, concept_id=1177480)

        # Assert
        self.assertIsNotNone(data)
        self.assertEqual(data["name"], "Enalapril")
        self.assertEqual(data["labels"], ["Concept", "Drug", "Standard"])
        self.assertIn("IS_A", data["relationships_summary"])
        self.assertEqual(data["ancestors_summary"]["count"], 1)
        self.assertIn(
            "Cardiovascular Agent", data["ancestors_summary"]["sample_ancestors"]
        )

    def test_verify_sample_concept_not_found(self):
        # Arrange
        mock_driver = MagicMock()
        mock_session = MagicMock()
        mock_result = MagicMock()
        # Simulate the case where no record is found
        mock_result.single.return_value = None
        mock_session.run.return_value.single.return_value = None
        mock_driver.session.return_value.__enter__.return_value = mock_session

        # Act
        data = validation.verify_sample_concept(mock_driver, concept_id=999)

        # Assert
        self.assertIsNone(data)

    @unittest.mock.patch("py_omop2neo4j_lpg.validation.get_driver", side_effect=Exception("mock connection error"))
    def test_run_validation_failure(self, mock_get_driver):
        """
        Tests that run_validation catches exceptions and returns an error dictionary.
        """
        result = validation.run_validation()
        self.assertIn("error", result)
        self.assertEqual(result["error"], "mock connection error")


if __name__ == "__main__":
    unittest.main()
