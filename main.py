import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.missing import MissingDatesFinder 
from src.extraction import EMAWebScraper
from src.transformation import TransformCSV
from src.loading import Loader
from src.gather_all import Gather


finder = MissingDatesFinder()
finder.run()

scraper = EMAWebScraper()
scraper.run()

transformer = TransformCSV()
transformer.run()

loader = Loader()
loader.run()

gather = Gather()
gather.run()