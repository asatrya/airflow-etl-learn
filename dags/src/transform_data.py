from airflow.models import Variable
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook

import os
import json
import numpy as np
import csv
import logging


GCS_BUCKET = Variable.get('GCS_BUCKET')

def transform_data(**kwargs):
    """
    Processes the json data, checks the types and save as csv
    """

    # Set source file
    source_file_name = str(kwargs["execution_date"]) + '.json'
    source_dir_path = os.path.join(os.path.dirname(__file__),
                            '..', '..',
                            'data',
                            kwargs["dag"].dag_id,
                            'get_weather')
    source_full_path = os.path.join(source_dir_path, source_file_name)

    # download from GCS
    gcs = GoogleCloudStorageHook('gcp_airflow_lab')
    gcs_src_object = os.path.join(kwargs["dag"].dag_id, 'get_weather', source_file_name)
    gcs.upload(GCS_BUCKET, 
            gcs_src_object, 
            source_full_path, 
            mime_type='application/octet-stream')
    logging.info("Successfully download file from GCS: gs://{}/{}".format(GCS_BUCKET, gcs_src_object))

    # open the json datafile and read it in
    with open(source_full_path, 'r') as inputfile:
        doc = json.load(inputfile)
        logging.info("Read from {}".format(source_full_path))
        logging.info("Content: {}".format(doc))

    # transform the data to the correct types and convert temp to celsius
    city = str(doc['name'])
    country = str(doc['sys']['country'])
    lat = float(doc['coord']['lat'])
    lon = float(doc['coord']['lon'])
    humid = float(doc['main']['humidity'])
    press = float(doc['main']['pressure'])
    min_temp = float(doc['main']['temp_min']) - 273.15
    max_temp = float(doc['main']['temp_max']) - 273.15
    temp = float(doc['main']['temp']) - 273.15
    weather = str(doc['weather'][0]['description'])
    todays_date = str(kwargs["execution_date"]).split('T')[0]

    # check for nan's in the numeric values and then enter into the database
    valid_data = True
    for valid in np.isnan([lat, lon, humid, press, min_temp, max_temp, temp]):
        if valid is False:
            valid_data = False
            raise ValueError('"Invalid data."')
            break

    row = [city, country, lat, lon, todays_date, humid, press, min_temp,
           max_temp, temp, weather]
    
    # Save output file
    dest_file_name = str(kwargs["execution_date"]) + '.csv'
    dest_dir_path = os.path.join(os.path.dirname(__file__),
                            '..', '..',
                            'data',
                            kwargs["dag"].dag_id,
                            kwargs["task"].task_id)
    if not os.path.exists(dest_dir_path):
        os.makedirs(dest_dir_path)
    dest_full_path = os.path.join(dest_dir_path, dest_file_name)

    with open(dest_full_path, 'w') as outputfile:
            writer = csv.writer(outputfile)
            writer.writerow(row)
    logging.info("Write to {}".format(dest_full_path))

    # upload to GCS
    gcs = GoogleCloudStorageHook('gcp_airflow_lab')
    gcs_dest_object = os.path.join(kwargs["dag"].dag_id, kwargs["task"].task_id, dest_file_name)
    gcs.upload(GCS_BUCKET, 
            gcs_dest_object, 
            dest_full_path, 
            mime_type='application/octet-stream')
    logging.info("Successfully write output file to GCS: gs://{}/{}".format(GCS_BUCKET, gcs_dest_object))


if __name__ == "__main__":
    transform_data()
