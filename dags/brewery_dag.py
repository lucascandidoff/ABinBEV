from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
import os

# Add scripts folder to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'scripts')))

from extract import extract_data
from transform_silver import transform_to_silver
from transform_gold import transform_to_gold

default_args = {
    'owner': 'airflow',
    'retries': 1,
}

with DAG(
    dag_id='brewery_data_pipeline',
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    description='Extract, transform and aggregate brewery data from Open Brewery DB',
) as dag:

    # Task to extract raw data and save to bronze layer
    extract_task = PythonOperator(
        task_id='extract_brewery_data',
        python_callable=extract_data,
    )

    # Task to transform data to silver layer (Parquet, partitioned by state)
    silver_task = PythonOperator(
        task_id='transform_to_silver',
        python_callable=transform_to_silver,
    )

    # Task to aggregate data to gold layer (count per brewery type and location)
    gold_task = PythonOperator(
        task_id='transform_to_gold',
        python_callable=transform_to_gold,
    )

    # Define task dependencies
    extract_task >> silver_task >> gold_task
