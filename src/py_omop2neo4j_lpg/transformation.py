# Copyright (c) 2025-2026 Gowtham Adamane Rao. All Rights Reserved.
#
# Licensed under the Prosperity Public License 3.0.0 (the "License").
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License in the LICENSE file at the root
# of this repository, or at: https://prosperitylicense.com/versions/3.0.0
#
# Commercial use beyond a 30-day trial requires a separate license.

import os
import pandas as pd
from .config import settings, get_logger
from .utils import standardize_label, standardize_reltype

logger = get_logger(__name__)


import click
from glob import glob

def prepare_for_bulk_import(chunk_size: int, import_dir: str):
    """
    Transforms extracted CSVs into a format suitable for neo4j-admin import.
    - Creates header files and data files for nodes and relationships.
    - Processes large files in chunks to manage memory usage.
    - Returns the neo4j-admin command to be executed.
    """
    source_dir = settings.EXPORT_DIR

    # --- Pre-flight check: Ensure source files exist ---
    csv_files = glob(os.path.join(source_dir, "*.csv"))
    if not csv_files:
        logger.warning(f"No CSV files found in '{source_dir}'. No files to process for bulk import.")
        click.echo("No files to process for bulk import.")
        return None

    os.makedirs(import_dir, exist_ok=True)
    logger.info(f"Preparing bulk import files in directory: {import_dir}")

    # --- Define File Paths ---
    paths = {
        # Input
        "domain_in": os.path.join(source_dir, "domain.csv"),
        "vocabulary_in": os.path.join(source_dir, "vocabulary.csv"),
        "concept_in": os.path.join(source_dir, "concepts_optimized.csv"),
        "relationship_in": os.path.join(source_dir, "concept_relationship.csv"),
        "ancestor_in": os.path.join(source_dir, "concept_ancestor.csv"),
        # Output Nodes
        "domain_nodes": os.path.join(import_dir, "nodes_domain.csv"),
        "vocabulary_nodes": os.path.join(import_dir, "nodes_vocabulary.csv"),
        "concept_nodes": os.path.join(import_dir, "nodes_concept.csv"),
        # Output Rels
        "in_domain_rels": os.path.join(import_dir, "rels_in_domain.csv"),
        "from_vocab_rels": os.path.join(import_dir, "rels_from_vocabulary.csv"),
        "semantic_rels": os.path.join(import_dir, "rels_semantic.csv"),
        "ancestor_rels": os.path.join(import_dir, "rels_ancestor.csv"),
    }

    # --- Clear previous import files ---
    for key, path in paths.items():
        if "in" not in key and os.path.exists(path):
            os.remove(path)
            logger.debug(f"Removed existing file: {path}")

    # --- Process Metadata (Small Files) ---
    logger.info("Processing Domain and Vocabulary nodes...")
    # Domains
    df_domain = pd.read_csv(paths["domain_in"], dtype=str)
    df_domain[":LABEL"] = "Domain"
    df_domain.rename(columns={"domain_id": ":ID"}, inplace=True)
    df_domain.to_csv(paths["domain_nodes"], index=False)

    # Vocabularies
    df_vocab = pd.read_csv(paths["vocabulary_in"], dtype=str)
    df_vocab[":LABEL"] = "Vocabulary"
    df_vocab.rename(columns={"vocabulary_id": ":ID"}, inplace=True)
    df_vocab.to_csv(paths["vocabulary_nodes"], index=False)
    logger.info("Metadata processing complete.")

    # --- Process Concepts (Chunked) ---
    logger.info(f"Processing concepts in chunks of {chunk_size}...")
    concept_cols = {
        "concept_id": ":ID",
        "concept_name": "name:string",
        "concept_code": "concept_code:string",
        "standard_concept": "standard_concept:string",
        "invalid_reason": "invalid_reason:string",
        "valid_start_date": "valid_start_date:date",
        "valid_end_date": "valid_end_date:date",
        "synonyms": "synonyms:string[]",
    }
    is_first_chunk = True
    for chunk in pd.read_csv(
        paths["concept_in"], chunksize=chunk_size, dtype=str, keep_default_na=False
    ):
        # Hold original columns for relationship creation
        original_chunk = chunk.copy()

        # 1. Prepare Concept Nodes
        chunk[":LABEL"] = "Concept;" + chunk["domain_id"].apply(standardize_label)
        chunk.loc[chunk["standard_concept"] == "S", ":LABEL"] += ";Standard"

        # Rename columns for neo4j-admin
        chunk.rename(columns=concept_cols, inplace=True)

        # Select and write node data
        node_data = chunk[list(concept_cols.values()) + [":LABEL"]]
        node_data.to_csv(
            paths["concept_nodes"], mode="a", index=False, header=is_first_chunk
        )

        # 2. Prepare Contextual Relationships
        # IN_DOMAIN
        rels_domain = original_chunk[["concept_id", "domain_id"]].copy()
        rels_domain.rename(
            columns={"concept_id": ":START_ID", "domain_id": ":END_ID"}, inplace=True
        )
        rels_domain[":TYPE"] = "IN_DOMAIN"
        rels_domain.to_csv(
            paths["in_domain_rels"], mode="a", index=False, header=is_first_chunk
        )

        # FROM_VOCABULARY
        rels_vocab = original_chunk[["concept_id", "vocabulary_id"]].copy()
        rels_vocab.rename(
            columns={"concept_id": ":START_ID", "vocabulary_id": ":END_ID"},
            inplace=True,
        )
        rels_vocab[":TYPE"] = "FROM_VOCABULARY"
        rels_vocab.to_csv(
            paths["from_vocab_rels"], mode="a", index=False, header=is_first_chunk
        )

        is_first_chunk = False
    logger.info("Concept processing complete.")

    # --- Process Semantic Relationships (Chunked) ---
    logger.info(f"Processing concept relationships in chunks of {chunk_size}...")
    is_first_chunk = True
    for chunk in pd.read_csv(
        paths["relationship_in"], chunksize=chunk_size, dtype=str, keep_default_na=False
    ):
        chunk.rename(
            columns={
                "concept_id_1": ":START_ID",
                "concept_id_2": ":END_ID",
                "valid_start_date": "valid_start_date:date",
                "valid_end_date": "valid_end_date:date",
                "invalid_reason": "invalid_reason:string",
            },
            inplace=True,
        )
        chunk[":TYPE"] = chunk["relationship_id"].apply(standardize_reltype)
        chunk.drop(columns=["relationship_id"], inplace=True)
        chunk.to_csv(
            paths["semantic_rels"], mode="a", index=False, header=is_first_chunk
        )
        is_first_chunk = False
    logger.info("Semantic relationship processing complete.")

    # --- Process Ancestor Relationships (Chunked) ---
    logger.info(f"Processing concept ancestors in chunks of {chunk_size}...")
    is_first_chunk = True
    for chunk in pd.read_csv(
        paths["ancestor_in"], chunksize=chunk_size, dtype=str, keep_default_na=False
    ):
        chunk.rename(
            columns={
                "descendant_concept_id": ":START_ID",
                "ancestor_concept_id": ":END_ID",
                "min_levels_of_separation": "min_levels:int",
                "max_levels_of_separation": "max_levels:int",
            },
            inplace=True,
        )
        chunk[":TYPE"] = "HAS_ANCESTOR"
        chunk.to_csv(
            paths["ancestor_rels"], mode="a", index=False, header=is_first_chunk
        )
        is_first_chunk = False
    logger.info("Ancestor relationship processing complete.")

    # --- Generate Final Command ---
    logger.info("Generating neo4j-admin command...")

    # NOTE: The file paths in the generated command are relative to the `import_dir`.
    # The user must ensure their Docker volume mounts this directory to the container's import path.
    node_files = [
        paths["domain_nodes"],
        paths["vocabulary_nodes"],
        paths["concept_nodes"],
    ]
    rel_files = [
        paths["in_domain_rels"],
        paths["from_vocab_rels"],
        paths["semantic_rels"],
        paths["ancestor_rels"],
    ]

    # Base command with common options
    command_parts = [
        "neo4j-admin database import full \\",
        "  --delimiter=',' \\",
        "  --array-delimiter='|' \\",
        "  --multiline-fields=true \\",
    ]

    # Add node files
    for path in node_files:
        # The path in the command should be relative to the neo4j import directory
        filename = os.path.basename(path)
        command_parts.append(f"  --nodes='{filename}' \\")

    # Add relationship files
    for path in rel_files:
        filename = os.path.basename(path)
        command_parts.append(f"  --relationships='{filename}' \\")

    command_parts.append("  neo4j")  # Target database name

    final_command = "\n".join(command_parts)
    # A bit of cleanup for cleaner presentation
    final_command = final_command.replace(os.path.sep, "/")
    logger.info(f"Generated neo4j-admin command:\n{final_command}")

    return final_command
