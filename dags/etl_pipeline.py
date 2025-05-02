from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id='etl_pipeline',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['etl'],
) as dag:

    bronze_task = BashOperator(
        task_id='run_bronze',
        bash_command='python /opt/airflow/scripts/source_to_bronze.py'
    )

    silver_task = BashOperator(
        task_id='run_silver',
        bash_command='python /opt/airflow/scripts/bronze_to_silver.py'
    )

    gold_task = BashOperator(
        task_id='run_gold',
        bash_command='python /opt/airflow/scripts/silver_to_gold.py'
    )

    bronze_task >> silver_task >> gold_task
