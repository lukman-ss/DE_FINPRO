
# import pandas as pd
from datetime import datetime, timedelta

class Transform:
    """
    This class provides methods for transforming currency data.

    It includes a method for transforming data obtained from the API.
    """

    @staticmethod
    def transform_currency_data(**kwargs):
        """
        Transform currency rate data.

        Args:
            **kwargs: Keyword arguments provided by Airflow.

        Returns:
            list: A list of dictionaries containing transformed currency rate data.

        This method takes the data retrieved from the API and transforms it into a list of dictionaries with specific fields.
        """
        ti = kwargs['ti']
        data = ti.xcom_pull(task_ids='extract_currency_rates')
        result = []

        for extract in data:
            data = extract['data']
            base_currency = extract['base_currency']
            time_last_update_unix = data['time_last_update_unix']
            currency_rates = data['conversion_rates']

            for currency, rate in currency_rates.items():
                data = {
                    'base_currency': base_currency,
                    'currency': currency,
                    'rates': rate,
                    'time_last_update_unix': Transform.date_formatted("%Y-%m-%d %H:%M:%S", time_last_update_unix)
                }
                result.append(data)
        return result

    @staticmethod
    def date_formatted(date_format, unix_timestamp):
        """
        Format a Unix timestamp into a specified date format.

        Args:
            date_format (str): The desired date format.
            unix_timestamp (int): The Unix timestamp to be formatted.

        Returns:
            str: The formatted date as a string.

        This method takes a Unix timestamp, converts it to a specific date format, and returns the formatted date as a string.
        """
        original_datetime = datetime.fromtimestamp(unix_timestamp)
        new_datetime = original_datetime + timedelta(hours=7)
        formatted_new_datetime = new_datetime.strftime(date_format)
        return formatted_new_datetime
