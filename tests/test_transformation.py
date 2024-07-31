import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.transformation import TransformCSV
import pytest 
from unittest.mock import patch, mock_open
import pandas as pd 
import csv 
import json 


# Sample configuration data for testing
config_data = {
    'bronze_dir': 'test_bronze_dir',
    'silver_dir': 'test_silver_dir',
    'root_dir': 'test_root_dir',
    'transformed_files_list': 'test_transformed_files_list.csv'
}

@pytest.fixture
def transform_csv():
    with patch('builtins.open', mock_open(read_data=json.dumps(config_data))):
        yield TransformCSV()

def test_scrapped_files(transform_csv):
    # Mock os.listdir to return a fixed list of files
    with patch('os.listdir', return_value=['file1.csv', 'file2.csv']):
        result = transform_csv.scrapped_files()
    assert result == ['file1.csv', 'file2.csv']

def test_transformed_files(transform_csv):
    # Mock reading from the CSV file
    mock_csv_data = "filename,status\nfile1.csv,Success\nfile2.csv,Failed\n"
    with patch('builtins.open', mock_open(read_data=mock_csv_data)):
        result = transform_csv.transformed_files()
    assert result == ['file1.csv']

def test_untransformed_files(transform_csv):
    # Mock the output of scrapped_files and transformed_files methods
    with patch.object(transform_csv, 'scrapped_files', return_value=['file1.csv', 'file2.csv']), \
        patch.object(transform_csv, 'transformed_files', return_value=['file1.csv']):
        result = transform_csv.untransformed_files()
    assert result == ['file2.csv']
