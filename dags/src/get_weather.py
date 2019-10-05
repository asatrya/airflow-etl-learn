import requests
import config
import json
from datetime import datetime
import os


def get_weather():
    """
    Query openweathermap.com's API and to get the weather for
    Jakarta, ID and then dump the json to the /src/data/ directory
    with the file name "<today's date>.json"
    """

    # My API key is defined in my config.py file.
    paramaters = {'q': 'Jakarta, ID', 'appid': config.API_KEY}

    result = requests.get(
        "http://api.openweathermap.org/data/2.5/weather?", paramaters)

    # If the API call was sucessful, get the json and dump it to a file with
    # today's date as the title.
    if result.status_code == 200:

        # Get the json data
        json_data = result.json()

        # Save output file
        file_name = str(os.environ['AIRFLOW_CTX_EXECUTION_DATE']) + '.json'
        dir_path = os.path.join(os.path.dirname(__file__),
                                '..', '..',
                                'data',
                                os.environ['AIRFLOW_CTX_DAG_ID'],
                                os.environ['AIRFLOW_CTX_TASK_ID'])
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
        tot_name = os.path.join(dir_path, file_name)

        with open(tot_name, 'w') as outputfile:
            json.dump(json_data, outputfile)
    else:
        print("Error In API call.")


if __name__ == "__main__":
    get_weather()
