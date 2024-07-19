import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.extraction import EMAWebScraper
import pytest

# This decorator defines a fixture function (scraper in this case) that provides a clean instance of EMAWebScraper for each test function that uses it. 
# This ensures that each test starts with a fresh instance of EMAWebScraper.
@pytest.fixture 
def scraper():
    # Create an instance of EMAWebScraper for testing
    return EMAWebScraper('/media/lucas/Files/2.Projetos/3.Solar/src/config.json')

@pytest.fixture 
def missing_file():
    return '/media/lucas/Files/2.Projetos/3.Solar/test_missing_days.csv'

def test_read_missing(scraper,missing_file):
    actual_days = scraper.read_missing(missing_file)
    max_actual_day = max(actual_days) 
    assert actual_days == ['2024-07-15', '2024-07-16','2024-07-17']
    assert max_actual_day == '2024-07-17'
    
