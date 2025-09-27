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
import unittest

import pandas as pd
from py_omop2neo4j_lpg.config import settings
from py_omop2neo4j_lpg.transformation import prepare_for_bulk_import


class TestTransformation(unittest.TestCase):
    def setUp(self):
        """Set up a temporary directory with dummy CSV files for testing."""
        self.test_export_dir = os.path.join(settings.EXPORT_DIR, "test_temp")
        self.test_import_dir = os.path.join("bulk_import", "test_temp")
        os.makedirs(self.test_export_dir, exist_ok=True)
        os.makedirs(self.test_import_dir, exist_ok=True)

        # --- Create Dummy Input CSVs ---
        # domains
        pd.DataFrame(
            {
                "domain_id": ["Drug", "Condition"],
                "domain_name": ["Drug", "Condition"],
                "domain_concept_id": ["1", "2"],
            }
        ).to_csv(os.path.join(self.test_export_dir, "domain.csv"), index=False)

        # vocabularies
        pd.DataFrame(
            {
                "vocabulary_id": ["RxNorm", "SNOMED"],
                "vocabulary_name": ["RxNorm", "SNOMED"],
                "vocabulary_reference": ["ref1", "ref2"],
                "vocabulary_version": ["v1", "v2"],
                "vocabulary_concept_id": ["101", "102"],
            }
        ).to_csv(os.path.join(self.test_export_dir, "vocabulary.csv"), index=False)

        # concepts_optimized
        pd.DataFrame(
            {
                "concept_id": [1001, 1002, 1003],
                "concept_name": ["Aspirin", "Headache", "Pain Killer"],
                "domain_id": ["Drug", "Condition", "Drug/Device"],
                "vocabulary_id": ["RxNorm", "SNOMED", "RxNorm"],
                "concept_class_id": ["Ingredient", "Finding", "Ingredient"],
                "standard_concept": ["S", "S", ""],
                "concept_code": ["A1", "B2", "C3"],
                "valid_start_date": ["2000-01-01", "2000-01-01", "2000-01-01"],
                "valid_end_date": ["2099-12-31", "2099-12-31", "2099-12-31"],
                "invalid_reason": ["", "", ""],
                "synonyms": ["acetylsalicylic acid", "", "pain reliever|analgesic"],
            }
        ).to_csv(
            os.path.join(self.test_export_dir, "concepts_optimized.csv"), index=False
        )

        # concept_relationship
        pd.DataFrame(
            {
                "concept_id_1": [1001, 1003],
                "concept_id_2": [1002, 1001],
                "relationship_id": ["treats", "maps to"],
                "valid_start_date": ["2000-01-01", "2000-01-01"],
                "valid_end_date": ["2099-12-31", "2099-12-31"],
                "invalid_reason": ["", ""],
            }
        ).to_csv(
            os.path.join(self.test_export_dir, "concept_relationship.csv"), index=False
        )

        # concept_ancestor
        pd.DataFrame(
            {
                "descendant_concept_id": [1001],
                "ancestor_concept_id": [1003],
                "min_levels_of_separation": [1],
                "max_levels_of_separation": [1],
            }
        ).to_csv(
            os.path.join(self.test_export_dir, "concept_ancestor.csv"), index=False
        )

        # Override settings to use test directory
        self.original_export_dir = settings.EXPORT_DIR
        settings.EXPORT_DIR = self.test_export_dir

    def tearDown(self):
        """Clean up temporary directories."""
        shutil.rmtree(self.test_export_dir)
        shutil.rmtree(self.test_import_dir)
        settings.EXPORT_DIR = self.original_export_dir

    def test_prepare_for_bulk_import(self):
        """Test the main transformation function."""
        # Execute the function
        command = prepare_for_bulk_import(chunk_size=2, import_dir=self.test_import_dir)

        # --- Assert Command ---
        self.assertIn("neo4j-admin database import full", command)
        # Check that the command refers to the correct data files.
        # Modern neo4j-admin expects these files to contain their own headers.
        self.assertIn("--nodes='nodes_concept.csv'", command)
        self.assertIn("--nodes='nodes_domain.csv'", command)
        self.assertIn("--nodes='nodes_vocabulary.csv'", command)
        self.assertIn("--relationships='rels_semantic.csv'", command)
        self.assertIn("--relationships='rels_ancestor.csv'", command)
        self.assertIn("--relationships='rels_in_domain.csv'", command)
        self.assertIn("--relationships='rels_from_vocabulary.csv'", command)

        # --- Assert All Files Are Created ---
        expected_files = [
            "nodes_concept.csv",
            "rels_semantic.csv",
            "rels_ancestor.csv",
            "nodes_domain.csv",
            "nodes_vocabulary.csv",
            "rels_in_domain.csv",
            "rels_from_vocabulary.csv",
        ]
        for f in expected_files:
            self.assertTrue(
                os.path.exists(os.path.join(self.test_import_dir, f)),
                f"{f} was not created",
            )

        # --- Assert Content of All Generated Files ---

        # Domain Nodes
        df_domain_nodes = pd.read_csv(
            os.path.join(self.test_import_dir, "nodes_domain.csv"), dtype=str
        )
        self.assertEqual(len(df_domain_nodes), 2)
        self.assertListEqual(
            list(df_domain_nodes.columns),
            [":ID", "domain_name", "domain_concept_id", ":LABEL"],
        )
        self.assertTrue(all(df_domain_nodes[":LABEL"] == "Domain"))
        self.assertEqual(
            df_domain_nodes[df_domain_nodes[":ID"] == "Drug"]["domain_name"].iloc[0],
            "Drug",
        )

        # Vocabulary Nodes
        df_vocab_nodes = pd.read_csv(
            os.path.join(self.test_import_dir, "nodes_vocabulary.csv"), dtype=str
        )
        self.assertEqual(len(df_vocab_nodes), 2)
        self.assertIn(":ID", df_vocab_nodes.columns)
        self.assertTrue(all(df_vocab_nodes[":LABEL"] == "Vocabulary"))
        self.assertEqual(
            df_vocab_nodes[df_vocab_nodes[":ID"] == "RxNorm"]["vocabulary_name"].iloc[
                0
            ],
            "RxNorm",
        )

        # Concept Nodes
        df_concept_nodes = pd.read_csv(
            os.path.join(self.test_import_dir, "nodes_concept.csv"), dtype=str
        )
        self.assertEqual(len(df_concept_nodes), 3)
        self.assertIn(":ID", df_concept_nodes.columns)
        self.assertIn(":LABEL", df_concept_nodes.columns)
        # Check standard concept label
        aspirin_row = df_concept_nodes[df_concept_nodes[":ID"] == "1001"]
        self.assertEqual(aspirin_row[":LABEL"].iloc[0], "Concept;Drug;Standard")
        # Check sanitized label for "Drug/Device"
        painkiller_row = df_concept_nodes[df_concept_nodes[":ID"] == "1003"]
        self.assertEqual(painkiller_row[":LABEL"].iloc[0], "Concept;DrugDevice")
        # Check synonyms format
        self.assertEqual(
            painkiller_row["synonyms:string[]"].iloc[0], "pain reliever|analgesic"
        )

        # IN_DOMAIN Relationships
        df_domain_rels = pd.read_csv(
            os.path.join(self.test_import_dir, "rels_in_domain.csv"), dtype=str
        )
        self.assertEqual(len(df_domain_rels), 3)
        self.assertListEqual(
            list(df_domain_rels.columns), [":START_ID", ":END_ID", ":TYPE"]
        )
        self.assertTrue(all(df_domain_rels[":TYPE"] == "IN_DOMAIN"))
        self.assertEqual(
            df_domain_rels[df_domain_rels[":START_ID"] == "1001"][":END_ID"].iloc[0],
            "Drug",
        )

        # FROM_VOCABULARY Relationships
        df_vocab_rels = pd.read_csv(
            os.path.join(self.test_import_dir, "rels_from_vocabulary.csv"), dtype=str
        )
        self.assertEqual(len(df_vocab_rels), 3)
        self.assertListEqual(
            list(df_vocab_rels.columns), [":START_ID", ":END_ID", ":TYPE"]
        )
        self.assertTrue(all(df_vocab_rels[":TYPE"] == "FROM_VOCABULARY"))
        self.assertEqual(
            df_vocab_rels[df_vocab_rels[":START_ID"] == "1001"][":END_ID"].iloc[0],
            "RxNorm",
        )

        # Semantic Relationships
        df_semantic_rels = pd.read_csv(
            os.path.join(self.test_import_dir, "rels_semantic.csv"), dtype=str
        )
        self.assertEqual(len(df_semantic_rels), 2)
        self.assertIn(":START_ID", df_semantic_rels.columns)
        self.assertIn(":END_ID", df_semantic_rels.columns)
        self.assertIn(":TYPE", df_semantic_rels.columns)
        # Check standardized reltype
        maps_to_row = df_semantic_rels[df_semantic_rels[":START_ID"] == "1003"]
        self.assertEqual(maps_to_row[":TYPE"].iloc[0], "MAPS_TO")

        # Ancestor Relationships
        df_ancestor_rels = pd.read_csv(
            os.path.join(self.test_import_dir, "rels_ancestor.csv"), dtype=str
        )
        self.assertEqual(len(df_ancestor_rels), 1)
        self.assertListEqual(
            list(df_ancestor_rels.columns),
            [":START_ID", ":END_ID", "min_levels:int", "max_levels:int", ":TYPE"],
        )
        self.assertEqual(df_ancestor_rels[":TYPE"].iloc[0], "HAS_ANCESTOR")
        self.assertEqual(df_ancestor_rels["min_levels:int"].iloc[0], "1")

    def test_prepare_for_bulk_import_cleanup(self):
        """Tests that old files are cleaned up before transformation."""
        # Create a dummy file that should be deleted
        dummy_file_path = os.path.join(self.test_import_dir, "nodes_concept.csv")
        with open(dummy_file_path, "w") as f:
            f.write("dummy content")

        self.assertTrue(os.path.exists(dummy_file_path))

        # Execute the function
        prepare_for_bulk_import(chunk_size=2, import_dir=self.test_import_dir)

        # The dummy file should be gone, and the new file should not be empty
        self.assertTrue(os.path.exists(dummy_file_path))
        with open(dummy_file_path) as f:
            content = f.read()
            self.assertNotEqual(content, "dummy content")
            self.assertIn(":ID", content)  # Check for header


if __name__ == "__main__":
    unittest.main()
