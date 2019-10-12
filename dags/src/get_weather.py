from airflow.models import Variable
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook

import requests
import json
from datetime import datetime
import os
import logging


API_KEY = Variable.get('OPEN_WEATHER_API_KEY')
GCS_BUCKET = Variable.get('GCS_BUCKET')

def get_weather(**kwargs):
    """
    Query openweathermap.com's API and to get the weather for
    Jakarta, ID and then dump the json to the /src/data/ directory
    with the file name "<today's date>.json"
    """

    # My API key is defined in my config.py file.
    paramaters = {'q': 'Jakarta, ID', 'appid': API_KEY}
    logging.info("API_KEY={}".format(API_KEY))

    result = requests.get(
        "http://api.openweathermap.org/data/2.5/weather?", paramaters)

    # If the API call was sucessful, get the json and dump it to a file with
    # today's date as the title.
    if result.status_code == 200:

        # Get the json data
        json_data = result.json()
        logging.info("Response from API: {}".format(json_data))

        # Save output file
        file_name = str(kwargs["execution_date"]) + '.json'
        dir_path = os.path.join(os.path.dirname(__file__),
                                '..', '..',
                                'data',
                                kwargs["dag"].dag_id,
                                kwargs["task"].task_id)
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
        tot_name = os.path.join(dir_path, file_name)
        logging.info("Will write output to {}".format(tot_name))

        with open(tot_name, 'w') as outputfile:
            json.dump(json_data, outputfile)
            logging.info("Successfully write local output file")

        # upload to GCS
        gcs = GoogleCloudStorageHook('gcp_airflow_lab')
        gcs_dest_object = os.path.join(kwargs["dag"].dag_id, kwargs["task"].task_id, file_name)
        gcs.upload(GCS_BUCKET, 
                gcs_dest_object, 
                tot_name, 
                mime_type='application/octet-stream')
        logging.info("Successfully write output file to GCS: gs://{}/{}".format(GCS_BUCKET, gcs_dest_object))
    else:
        raise ValueError('"Error In API call."')


if __name__ == "__main__":
    get_weather()
