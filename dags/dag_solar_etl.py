import sys
import os
from datetime import datetime
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# Ensure the src directory is in the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.missing import MissingDatesFinder 
from src.extraction import EMAWebScraper
from src.transformation import TransformCSV
from src.loading import Loader
from src.gather_all import Gather

@dag(
    start_date=days_ago(1),
    schedule="@daily",
    catchup=False,
    doc_md="This DAG runs a pipeline of tasks: missing dates detection, web scraping, CSV transformation, data loading, and data gathering.",
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