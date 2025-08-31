import io
import os
import sys
import pandas as pd
import pytest

# Ensure the etl_pipeline_logic module is importable
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import etl_pipeline_logic


def test_assess_raw_data_basic():
    # Prepare a simple CSV byte content
    content = b"col1,col2\n1,2\n3,4"
    result = etl_pipeline_logic.assess_raw_data(content, "test.csv")

    assert result["file_name"] == "test.csv"
    assert result["working_encoding"] == "utf-8"
    assert result["likely_delimiter"] == ","
    assert isinstance(result["issues_found"], list)
    # No issues expected for a valid UTF-8 CSV
    assert result["issues_found"] == []


def test_clean_csv_data_basic():
    # Prepare simple CSV byte content for cleaning
    content = b"col1,col2\n1,2\n3,4"
    assessment = {"likely_delimiter": ",", "working_encoding": "utf-8"}
    df, report = etl_pipeline_logic.clean_csv_data(content, "test.csv", assessment_results=assessment)

    # Validate DataFrame output
    assert isinstance(df, pd.DataFrame)
    # The pipeline always adds a job_id column even if no URL column exists
    expected_cols = ["col1", "col2", "job_id"]
    assert list(df.columns) == expected_cols
    # Two data rows expected
    assert df.shape == (2, len(expected_cols))

    # Validate report DataFrame
    assert isinstance(report, pd.DataFrame)
    for col in ["original_column", "missing_percent_before", "cleaned_column", "action", "details"]:
        assert col in report.columns

    # Check that job_id column is reported
    assert any(report["cleaned_column"] == "job_id")
