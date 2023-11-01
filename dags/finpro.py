from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

# Import your custom modules for extraction, transformation, and loading
from extract import Extract
from transform import Transform
from load import Load

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 10, 18),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Instantiate the DAG and specify its schedule interval
with DAG('etl_currency_rates_dag', default_args=default_args, schedule_interval='@hourly', catchup=False) as dag:
    # Define a starting dummy task
    start_task = DummyOperator(task_id='start_task')

    # Define the task for extracting currency rates
    extract_task = PythonOperator(
        task_id='extract_currency_rates',
        python_callable=Extract.extract_currency_rates,
        op_args=[],
    )

    # Define the task for transforming currency data
    transform_task = PythonOperator(
        task_id='transform_currency_data',
        python_callable=Transform.transform_currency_data,
        provide_context=True,
    )

    # Define the task for loading currency data to staging
    load_task_staging = PythonOperator(
        task_id='load_currency_data',
        python_callable=Load.load_currency_data,
        provide_context=True,
    )

    # Define the task for loading currency data to a PostgreSQL database (neon)
    load_task_bq = PythonOperator(
        task_id='load_currency_pg',
        python_callable=Load.load_currency_data_bigquery,
        provide_context=True,
    )

    # Define an ending dummy task
    end_task = DummyOperator(task_id='end_task')

    # Set up the task dependencies by defining the execution order
    start_task >> extract_task >> transform_task >> [load_task_staging , load_task_bq] >> end_task

# Note: Make sure you have the necessary import statements, functions, and modules (Extract, Transform, Load) defined in your custom scripts (extract.py, transform.py, load.py)
