import sys
import os
from datetime import datetime
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator

# Ensure the src directory is in the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.missing import MissingDatesFinder 
from src.extraction import EMAWebScraper
from src.transformation import TransformCSV
from src.loading import Loader
from src.gather_all import Gather

@dag(
    start_date=datetime(2024, 8, 2, 21),
    schedule="@daily",
    catchup=False,
    doc_md="This DAG runs a pipeline of tasks: missing dates detection, web scraping, CSV transformation, data loading, and data gathering.",
    default_args={"owner": "Astro", "retries": 3},
    tags=["webscrapping"],
)
def pipeline():
    @task
    def missing():
        finder = MissingDatesFinder()
        finder.run()
        
    @task 
    def extraction():
        scraper = EMAWebScraper()
        scraper.run()
    
    @task 
    def transforming():
        transformer = TransformCSV()
        transformer.run()    
        
    @task 
    def loading():
        loader = Loader()
        loader.run()
    
    @task 
    def gathering():
        gather = Gather()
        gather.run()        
    
    # Set task dependencies
    missing() >> extraction() >> transforming() >> loading() >> gathering()

pipeline_dag = pipeline()
