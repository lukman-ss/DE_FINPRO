import requests
from airflow.models import Variable

class Extract:
    """
    This class provides a method for extracting currency rates from an API.

    It retrieves currency rates for the specified currency codes and returns the data as a list of dictionaries.
    """

    @staticmethod
    def extract_currency_rates():
        """
        Extract currency rates from the specified API.

        Returns:
            list: A list of dictionaries, each containing the base currency code and currency rate data.

        This method retrieves currency rates for the specified currency codes and returns the data as a list of dictionaries.
        """
        cc_airflow = Variable.get("CURRENCY_CODE")
        currency_codes = cc_airflow.split(',')
        result = []

        for currency_code in currency_codes:
            url = Variable.get("ENDPOINT") + currency_code
            response = requests.get(url)

            if response.status_code == 200:
                result.append({'base_currency': currency_code, 'data': response.json()})
            else:
                print(f"Failed to fetch data for {currency_code}. Status code: {response.status_code}")
        return result