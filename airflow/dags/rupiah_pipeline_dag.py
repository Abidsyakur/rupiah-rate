"""Airflow DAG for Rupiah pipeline"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email': ['data-alerts@company.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'rupiah_pipeline_dag',
    default_args=default_args,
    description='Daily Rupiah Exchange Rate ETL Pipeline',
    schedule_interval='0 10 * * *',  # 10 AM daily
    catchup=False,
    tags=['rupiah', 'etl', 'daily'],
)


def extract_data(**context):
    """Extract exchange rate data"""
    print("Extracting data...")
    # TODO: Implement extraction logic


def transform_data(**context):
    """Transform and validate data"""
    print("Transforming data...")
    # TODO: Implement transformation logic


def load_data(**context):
    """Load data into warehouse"""
    print("Loading data...")
    # TODO: Implement loading logic


# Tasks
extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag,
)

dbt_task = BashOperator(
    task_id='run_dbt',
    bash_command='cd /app/dbt && dbt run --profiles-dir .',
    dag=dag,
)

# Dependencies
extract_task >> transform_task >> load_task >> dbt_task
