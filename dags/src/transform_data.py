import os
import json
import numpy as np
import csv


def transform_data():
    """
    Processes the json data, checks the types and save as csv
    """
    
    # Read file from previous task
    source_file_name = str(os.environ['AIRFLOW_CTX_EXECUTION_DATE']) + '.json'
    source_dir_path = os.path.join(os.path.dirname(__file__),
                            '..', '..',
                            'data',
                            os.environ['AIRFLOW_CTX_DAG_ID'],
                            'get_weather')
    source_full_path = os.path.join(source_dir_path, source_file_name)

    # open the json datafile and read it in
    with open(source_full_path, 'r') as inputfile:
        doc = json.load(inputfile)

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
    todays_date = os.environ['AIRFLOW_CTX_EXECUTION_DATE'].split('T')[0]

    # check for nan's in the numeric values and then enter into the database
    valid_data = True
    for valid in np.isnan([lat, lon, humid, press, min_temp, max_temp, temp]):
        if valid is False:
            valid_data = False
            break

    row = [city, country, lat, lon, todays_date, humid, press, min_temp,
           max_temp, temp, weather]
    
    # Save output file
    dest_file_name = str(os.environ['AIRFLOW_CTX_EXECUTION_DATE']) + '.csv'
    dest_dir_path = os.path.join(os.path.dirname(__file__),
                            '..', '..',
                            'data',
                            os.environ['AIRFLOW_CTX_DAG_ID'],
                            os.environ['AIRFLOW_CTX_TASK_ID'])
    if not os.path.exists(dest_dir_path):
        os.makedirs(dest_dir_path)
    dest_full_path = os.path.join(dest_dir_path, dest_file_name)

    with open(dest_full_path, 'w') as outputfile:
            writer = csv.writer(outputfile)
            writer.writerow(row)


if __name__ == "__main__":
    transform_data()
