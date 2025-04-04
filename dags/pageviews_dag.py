from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from helper import (analyze_data, download_data, extract_data, load_data,
                    transform_data)

default_args = {
    'owner': 'Ayoub',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,  # Prevent backfilling of missed runs
}

# DAG definition
dag = DAG(
    'pageviews_dag',
    default_args=default_args,
    description='A DAG to download, extract, \
        transform, and load pageviews data',
    schedule_interval=None,
    max_active_runs=1,  # Limit to one active run at a time
)

# Constants
WIKI_URL = (
    "https://dumps.wikimedia.org/other/pageviews/2025/2025-01/"
    "pageviews-20250101-000000.gz"
)

COMPANIES = ['Google', 'Facebook', 'Amazon', 'Apple', 'Microsoft']

# definte the tasks
# Task 1: Download the data
download_task = PythonOperator(
    task_id='download_data',
    python_callable=download_data,
    op_args=[WIKI_URL],
    dag=dag,
)

# Task 2: Extract the pageviews
extract_task = PythonOperator(
    task_id='extract_pageviews',
    python_callable=extract_data,
    op_args=['data/pageviews.gz'],
    dag=dag
)

# Task 3: Filter the pageviews based on the companies
filter_task = PythonOperator(
    task_id='filter_pageviews',
    python_callable=transform_data,
    op_args=[COMPANIES, 'data/pageviews.txt'],  # the extracted file
    dag=dag
)

# Task 4: Load the filtered data into the database
load_task = PythonOperator(
    task_id='load_pageviews',
    python_callable=load_data,
    op_args=['data/filtered_pageviews.csv', None, 'postgresql://airflow:airflow@postgres/airflow'],
    dag=dag
)

# Task 5: Analyze the data
analyze_task = PythonOperator(
    task_id='analyze_pageviews',
    python_callable=analyze_data,
    op_args=[None, 'postgresql://airflow:airflow@postgres/airflow'],
    dag=dag
)

# Defining task dependencies
download_task >> extract_task >> filter_task >> load_task >> analyze_task
