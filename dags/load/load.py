from airflow.hooks.postgres_hook import PostgresHook
import pandas as pd
import time
from datetime import datetime, timedelta
import os
from airflow.models import Variable
from google.cloud import bigquery

class Load:
    """
    This class provides methods for loading currency data into different destinations.

    It includes methods for loading data to a CSV file and to a PostgreSQL database.
    """
    @staticmethod
    def load_currency_data_bigquery(**kwargs):
        """
        Load currency data to Google BigQuery.

        Args:
            **kwargs: Keyword arguments provided by Airflow.

        This function pulls data from the 'transform_currency_data' task and inserts it into Google BigQuery.

        """
        ti = kwargs['ti']
        data_list = ti.xcom_pull(task_ids='transform_currency_data')

        # Construct a BigQuery client object.
        client = bigquery.Client()

        # TODO(developer): set project dataset dan table di variable
        table = Variable.get("PROJECT") + '.' +Variable.get("DATASET") + '.' + Variable.get("TABLE") 

        dataframe = pd.DataFrame(data_list)
        job_config = bigquery.LoadJobConfig(
            autodetect=True,
            write_disposition="WRITE_APPEND",
        )

        job = client.load_table_from_dataframe(
            dataframe, table, job_config=job_config
        )  # Make an API request.
        job.result()  # Wait for the job to complete.

        table = client.get_table(table)  # Make an API request.
        print(
            "Loaded {} rows and {} columns to {}".format(
                table.num_rows, len(table.schema), table
            )
        )

    @staticmethod
    def load_currency_data(**kwargs):
        """
        Load currency data to a CSV file.

        Args:
            **kwargs: Keyword arguments provided by Airflow.

        This function pulls data from the 'transform_currency_data' task and saves it to a CSV file.

        """
        ti = kwargs['ti']
        data_list = ti.xcom_pull(task_ids='transform_currency_data')
        now = int(time.time())
        extract_df = pd.DataFrame(data_list)

        csv_file_path = Variable.get("PATH_SAVE") + str(Load.date_formatted("%Y-%m-%d %H", now)) + ".csv"
        # Save the DataFrame to a CSV file
        Load.save_csv_with_backup(csv_file_path, extract_df)

    @staticmethod
    def load_currency_data_pg_neon(**kwargs):
        """
        Load currency data to a PostgreSQL database.

        Args:
            **kwargs: Keyword arguments provided by Airflow.

        This function pulls data from the 'transform_currency_data' task and inserts it into a PostgreSQL database.

        """
        ti = kwargs['ti']
        data_list = ti.xcom_pull(task_ids='transform_currency_data')
        df = pd.DataFrame(data_list)
        pg_hook = PostgresHook(postgres_conn_id='neondb_pg')
        pg_hook.insert_rows(table=Variable.get("NEON_DB_TABLE"), schema=Variable.get("NEON_DB_SCHEMA"), rows=df.values, target_fields=list(df.columns))
        
        connection_uri = PostgresHook(postgres_conn_id='neondb_pg').get_uri()
        connection_uri_with_ssl = connection_uri + "?sslmode=require"

        pg_hook = PostgresHook(postgres_conn_id='neondb_pg', conn_type="gcp_sql", sslmode="require")
        pg_hook.insert_rows(table=Variable.get("NEON_DB_TABLE"), schema=Variable.get("NEON_DB_SCHEMA"), rows=df.values, target_fields=list(df.columns))

    @staticmethod
    def save_csv_with_backup(csv_file_path, data):
        """
        Save DataFrame to a CSV file with backup and folder creation.

        Args:
            csv_file_path (str): The path to the CSV file.
            data (DataFrame): The data to be saved to the CSV file.

        This function creates a backup of the CSV file if it already exists and ensures that the folder structure exists.

        """
        folder_path = os.path.dirname(csv_file_path)

        if not os.path.exists(folder_path):
            os.makedirs(folder_path)

        if os.path.isfile(csv_file_path):
            print(f"CSV file overwritten at: {csv_file_path}")
        else:
            print(f"CSV file created at: {csv_file_path}")

        data.to_csv(csv_file_path, index=False)

    @staticmethod
    def date_formatted(date_format, unix_timestamp):
        """
        Format a Unix timestamp into a specified date format.

        Args:
            date_format (str): The desired date format.
            unix_timestamp (int): The Unix timestamp to be formatted.

        Returns:
            str: The formatted date as a string.

        This function takes a Unix timestamp, converts it to a specific date format, and returns the formatted date as a string.

        """
        original_datetime = datetime.fromtimestamp(unix_timestamp)
        new_datetime = original_datetime + timedelta(hours=7)
        formatted_new_datetime = new_datetime.strftime(date_format)
        return formatted_new_datetime
