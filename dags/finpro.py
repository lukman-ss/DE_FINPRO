import pandas as pd
from datetime import datetime, timedelta
import requests
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import os
import time

default_args = {
    'owner': 'owner',
    'start_date': datetime(2023, 10, 18),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
def save_csv_with_backup(csv_file_path, data):
    folder_path = os.path.dirname(csv_file_path)

    if not os.path.exists(folder_path):
        os.makedirs(folder_path)

    if os.path.isfile(csv_file_path):
        print(f"CSV file overwritten at: {csv_file_path}")
    else:
        print(f"CSV file created at: {csv_file_path}")

    data.to_csv(csv_file_path, index=False)

def extract_currency_rates():
    url = 'https://open.er-api.com/v6/latest/USD'
    response = requests.get(url)
    data = response.json()
    return data

def transform_currency_data(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='extract_currency_rates')
    data_list = [{'Currency': currency, 'Rate': rate, 'time_last_update_unix': data["time_last_update_unix"]} for currency, rate in data['rates'].items()]
    return data_list

def load_currency_data(**kwargs):
    ti = kwargs['ti']
    data_list = ti.xcom_pull(task_ids='transform_currency_data')
    
    df = pd.DataFrame(data_list)
    
    csv_file_path = "/home/airflow/gcs/data/currency/currency_rates.csv"
    csv_file_path2 = "/home/airflow/gcs/data/currency/"+ str(time.time()) + "/currency_rates.csv"

    save_csv_with_backup(csv_file_path, df)
    save_csv_with_backup(csv_file_path2, df)

with DAG('etl_currency_rates_dag', default_args=default_args, schedule_interval='@hourly', catchup=False) as dag:
    extract_task = PythonOperator(
        task_id='extract_currency_rates',
        python_callable=extract_currency_rates
    )

    transform_task = PythonOperator(
        task_id='transform_currency_data',
        python_callable=transform_currency_data,
        provide_context=True
    )

    load_task = PythonOperator(
        task_id='load_currency_data',
        python_callable=load_currency_data,
        provide_context=True
    )

    extract_task >> transform_task >> load_task
