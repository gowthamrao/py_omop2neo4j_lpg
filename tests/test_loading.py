# Copyright (c) 2025-2026 Gowtham Adamane Rao. All Rights Reserved.
#
# Licensed under the Prosperity Public License 3.0.0 (the "License").
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License in the LICENSE file at the root
# of this repository, or at: https://prosperitylicense.com/versions/3.0.0
#
# Commercial use beyond a 30-day trial requires a separate license.

import pytest
from unittest.mock import MagicMock, patch
from py_omop2neo4j_lpg import loading

def test_execute_queries_raises_on_error():
    """
    Tests that _execute_queries re-raises an exception when ignore_errors is False.
    """
    mock_driver = MagicMock()
    mock_session = MagicMock()
    mock_driver.session.return_value.__enter__.return_value = mock_session
    mock_session.run.side_effect = Exception("mock db error")

    with pytest.raises(Exception, match="mock db error"):
        loading._execute_queries(mock_driver, ["FAILING QUERY"], ignore_errors=False)

import os
from py_omop2neo4j_lpg.config import settings

@patch("py_omop2neo4j_lpg.loading.get_driver")
@patch("py_omop2neo4j_lpg.loading.clear_database", side_effect=Exception("mock clear error"))
def test_run_load_csv_failure(mock_clear_db, mock_get_driver, tmp_path):
    """
    Tests that run_load_csv catches and re-raises exceptions from downstream processes.
    """
    # Create a dummy CSV file to bypass the pre-flight check
    export_dir = settings.EXPORT_DIR
    os.makedirs(export_dir, exist_ok=True)
    dummy_csv_path = os.path.join(export_dir, "dummy.csv")
    with open(dummy_csv_path, "w") as f:
        f.write("id,name\n1,test")

    with pytest.raises(Exception, match="mock clear error"):
        loading.run_load_csv()

    # Clean up the dummy file
    os.remove(dummy_csv_path)
