import io
import os
import sys
import pandas as pd
import pytest
from unittest.mock import Mock, patch, MagicMock

# Ensure the etl_pipeline_logic module is importable
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import etl_pipeline_logic
import bigquery_config


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


def test_validate_bigquery_config_valid():
    """Test BigQuery config validation with valid configuration"""
    valid_config = {
        'project': 'my-project-123',
        'dataset': 'my_dataset',
        'table': 'my_table'
    }
    
    is_valid, message = etl_pipeline_logic.validate_bigquery_config(valid_config)
    assert is_valid == True
    assert "valid" in message.lower()


def test_validate_bigquery_config_missing_fields():
    """Test BigQuery config validation with missing required fields"""
    
    # Missing project
    config = {'dataset': 'my_dataset', 'table': 'my_table'}
    is_valid, message = etl_pipeline_logic.validate_bigquery_config(config)
    assert is_valid == False
    assert "project" in message
    
    # Missing dataset
    config = {'project': 'my-project', 'table': 'my_table'}
    is_valid, message = etl_pipeline_logic.validate_bigquery_config(config)
    assert is_valid == False
    assert "dataset" in message
    
    # Missing table
    config = {'project': 'my-project', 'dataset': 'my_dataset'}
    is_valid, message = etl_pipeline_logic.validate_bigquery_config(config)
    assert is_valid == False
    assert "table" in message


def test_validate_bigquery_config_invalid_names():
    """Test BigQuery config validation with invalid naming"""
    
    # Invalid dataset name (starts with number)
    config = {
        'project': 'my-project',
        'dataset': '123invalid',
        'table': 'my_table'
    }
    is_valid, message = etl_pipeline_logic.validate_bigquery_config(config)
    assert is_valid == False
    assert "dataset" in message.lower()


@patch('google.cloud.bigquery.Client')
@patch('google.cloud.bigquery.Dataset')
@patch('google.cloud.bigquery.LoadJobConfig')
def test_upload_to_bigquery_success(mock_load_config, mock_dataset_class, mock_client_class):
    """Test successful BigQuery upload"""
    
    # Mock the BigQuery client and operations
    mock_client = Mock()
    mock_dataset = Mock()
    mock_job = Mock()
    mock_job.result.return_value = None
    
    mock_client_class.return_value = mock_client
    mock_client.get_dataset.return_value = mock_dataset
    mock_client.load_table_from_dataframe.return_value = mock_job
    
    # Test data
    df = pd.DataFrame({'col1': [1, 2], 'col2': [3, 4]})
    config = {
        'project': 'test-project',
        'dataset': 'test_dataset', 
        'table': 'test_table'
    }
    
    result = etl_pipeline_logic.upload_to_bigquery(df, config)
    
    assert result['success'] == True
    assert result['table_id'] == 'test-project.test_dataset.test_table'
    assert result['rows_uploaded'] == 2


@patch('google.cloud.bigquery.Client')
@patch('google.cloud.bigquery.Dataset')
@patch('google.cloud.bigquery.LoadJobConfig')
def test_upload_to_bigquery_create_dataset(mock_load_config, mock_dataset_class, mock_client_class):
    """Test BigQuery upload with dataset creation"""
    
    mock_client = Mock()
    mock_job = Mock()
    mock_job.result.return_value = None
    
    # Simulate dataset not existing (get_dataset raises exception)
    mock_client.get_dataset.side_effect = Exception("Dataset not found")
    mock_client.create_dataset.return_value = None
    mock_client.load_table_from_dataframe.return_value = mock_job
    
    mock_client_class.return_value = mock_client
    
    df = pd.DataFrame({'col1': [1, 2]})
    config = {
        'project': 'test-project',
        'dataset': 'new_dataset',
        'table': 'test_table'
    }
    
    result = etl_pipeline_logic.upload_to_bigquery(df, config)
    
    assert result['success'] == True
    # Verify dataset creation was attempted
    mock_client.create_dataset.assert_called_once()


def test_upload_to_bigquery_invalid_config():
    """Test BigQuery upload with invalid configuration"""
    
    df = pd.DataFrame({'col1': [1, 2]})
    invalid_config = {'project': 'test'}  # Missing dataset and table
    
    result = etl_pipeline_logic.upload_to_bigquery(df, invalid_config)
    
    assert result['success'] == False
    assert 'Configuration validation failed' in result['error']


def test_bigquery_config_management():
    """Test BigQuery configuration save/load functionality"""
    
    test_config = {
        'project': 'test-project',
        'dataset': 'test_dataset',
        'table': 'test_table'
    }
    
    # Test saving config
    config_file = 'test_config.json'
    try:
        success = bigquery_config.save_bigquery_config(test_config, config_file)
        assert success == True
        assert os.path.exists(config_file)
        
        # Test loading config
        loaded_config = bigquery_config.load_bigquery_config(config_file)
        assert loaded_config == test_config
        
    finally:
        # Cleanup
        if os.path.exists(config_file):
            os.remove(config_file)


def test_setup_instructions():
    """Test that setup instructions are available"""
    instructions = bigquery_config.get_setup_instructions()
    
    assert isinstance(instructions, str)
    assert len(instructions) > 100
    assert "BigQuery" in instructions
    assert "authentication" in instructions.lower()
    assert "service account" in instructions.lower()