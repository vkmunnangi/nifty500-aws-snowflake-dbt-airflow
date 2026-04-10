import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator

# Ensure the MWAA worker can find the src modulo structure when dags/ and src/ are zipped
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.extractors.yfinance_to_s3 import main as run_yfinance_extraction

default_args = {
    'owner': 'data-engineering-team',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='nifty500_lakehouse_pipeline',
    default_args=default_args,
    description='Extracts Nifty 500 OHLCV data into S3 and triggers dbt Cloud Silver/Gold transformations.',
    schedule='0 12 * * 1-5',  # 12:00 PM UTC = 5:30 PM IST (Mon-Fri)
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['finance', 'dbt', 'snowflake', 's3'],
) as dag:

    # -----------------------------------------------------------------------------------
    # Task 1: Ingest Bronze (Extract & Load)
    # -----------------------------------------------------------------------------------
    # Executes the python script gracefully using Airflow Connections/Variables loaded
    # directly via the refactored script.
    extract_bronze_to_s3 = PythonOperator(
        task_id='extract_yfinance_to_bronze_s3',
        python_callable=run_yfinance_extraction,
        execution_timeout=timedelta(minutes=45), # Extended timeout for massive backfills
    )

    # -----------------------------------------------------------------------------------
    # Task 2: Transform Silver & Gold (dbt Cloud)
    # -----------------------------------------------------------------------------------
    # Triggers the dbt Cloud Job instantly.
    # Note: Requires setting up the Airflow Connection `dbt_cloud_default` in MWAA UI.
    transform_silver_gold = DbtCloudRunJobOperator(
        task_id='trigger_dbt_cloud_transformations',
        # dbt Cloud Job ID: Nifty500 Daily Pipeline
        job_id=70471823582979,
        check_interval=30, 
        timeout=1800, # Wait up to 30 mins for the transformation to succeed
        wait_for_termination=True, # Airflow task stays yellow until dbt finishes
        dbt_cloud_conn_id='dbt_cloud_default',
    )

    # Define execution order
    extract_bronze_to_s3 >> transform_silver_gold
