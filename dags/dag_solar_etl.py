from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Import the module
from missing_json import main as find_missing_dates

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 18),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'dag_missing_json',
    default_args=default_args,
    description='A DAG to find missing JSON dates and store them in a file',
    schedule_interval=timedelta(days=1),
    catchup=False,
) as dag:

    # Task to find and save missing dates
    task_find_missing_dates = PythonOperator(
        task_id='find_missing_dates',
        python_callable=find_missing_dates,
    )

    task_find_missing_dates
