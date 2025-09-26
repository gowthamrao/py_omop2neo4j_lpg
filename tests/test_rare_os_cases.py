# Copyright (c) 2025-2026 Gowtham Adamane Rao. All Rights Reserved.
#
# Licensed under the Prosperity Public License 3.0.0 (the "License").
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License in the LICENSE file at the root
# of this repository, or at: https://prosperitylicense.com/versions/3.0.0
#
# Commercial use beyond a 30-day trial requires a separate license.

import os
import tempfile
import shutil
import pytest
from click.testing import CliRunner
from unittest.mock import patch

from py_omop2neo4j_lpg.cli import cli
from py_omop2neo4j_lpg.config import settings


@pytest.mark.integration
def test_extract_with_path_with_spaces(pristine_db):
    """
    Tests that the `extract` command works correctly when the EXPORT_DIR
    contains spaces in its path, a common scenario on Windows.
    """
    runner = CliRunner()
    # Create a temporary directory with a space in its name
    base_dir = tempfile.mkdtemp()
    spaced_dir = os.path.join(base_dir, "export dir with spaces")
    os.makedirs(spaced_dir)

    try:
        with patch.object(settings, "EXPORT_DIR", spaced_dir):
            result = runner.invoke(cli, ["extract"])
            assert result.exit_code == 0, f"Extract failed: {result.output}"

            # Check that a representative file was created in the correct directory
            expected_file = os.path.join(spaced_dir, "concepts_optimized.csv")
            assert os.path.exists(expected_file), f"File not found in spaced directory: {expected_file}"
    finally:
        # Clean up the temporary directory
        shutil.rmtree(base_dir)