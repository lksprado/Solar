import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.extraction import EMAWebScraper
import pytest
import json
from unittest.mock import patch, MagicMock

# TEST BREAKDOWN

# tmpdir: FIXTURE THAT CREATES A TEMPORARY DIRECTORY
# config data: DICTIONARY WITH MOCK DATA
# config_file: CREATES THE FULL PATH FOR THE config_data INT THE tmpdir FOLDER
# OPENS config_data as JSON
# RETURNS THE FILE PATH AS STRING TO BE USED FOR MORE TESTS WITH THIS FIXTURE
@pytest.fixture
def mock_config_file(tmpdir):
    config_data = {
        'username': 'test_user',
        'password': 'test_pass',
        'jsondir': 'path/to/json',
        'servicedir': 'path/to/service'
    }
    config_file = tmpdir.join('config.json')
    with open(config_file, 'w') as f:
        json.dump(config_data, f)
    return str(config_file)

@pytest.fixture
def mock_missing_file(tmpdir):
    missing_data = [
        'title\n','2024-07-15\n','2024-07-16\n','2024-07-17\n'
    ]
    missing_file = tmpdir.join('missing.csv')
    with open(missing_file, 'w') as f:
        f.writelines(missing_data)
    return str(missing_file)


# THE TESTS USES THE FIXTURE
# CREATES AN INSTANCE FOR EMAWEBSCRAPPER USING THE FILE PATH OF CONFIG FILE
# ASSERTS IF THE METHODS READS THE DATA CORRECTLY MATCHING THE FIXTURE
def test_read_config(mock_config_file):
    scraper = EMAWebScraper(config_file=mock_config_file)
    assert scraper.config['username'] == 'test_user'
    assert scraper.config['password'] == 'test_pass'
    assert scraper.config['jsondir'] == 'path/to/json'
    assert scraper.config['servicedir'] == 'path/to/service'
    
def test_read_missing(mock_missing_file,  mock_config_file):
    scraper = EMAWebScraper(config_file=mock_config_file)
    days = scraper.read_missing(mock_missing_file)
    expected_days = ['2024-07-15', '2024-07-16', '2024-07-17']
    assert days == expected_days


@patch('src.extraction.webdriver.Edge')
def test_login(mock_edge, mock_config_file):
    mock_driver = MagicMock()
    mock_edge.return_value = mock_driver
    scraper = EMAWebScraper(config_file=mock_config_file)
    scraper.driver = mock_driver
    with patch('src.extraction.WebDriverWait') as mock_wait:
        mock_wait.return_value.until.return_value = MagicMock()
        scraper.login()
        assert mock_wait.call_count >= 3
        mock_wait.return_value.until.assert_called()
