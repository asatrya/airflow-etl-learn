from airflow.models import Variable
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.hooks.postgres_hook import PostgresHook

import os
import numpy as np
import csv
import logging


GCS_BUCKET = Variable.get('GCS_BUCKET')

def load_table(**kwargs):
    """
    Processes the json data, checks the types and enters into the
    Postgres database.
    """

    pg_hook = PostgresHook(postgres_conn_id='weatherdb_postgres_conn')
    gcs = GoogleCloudStorageHook('gcp_airflow_lab')
    prev_task_id = 'transform_data'

    # Set source file
    source_file_name = str(kwargs["execution_date"]) + '.csv'
    source_dir_path = os.path.join(os.path.dirname(__file__),
                                   '..', '..',
                                   'data',
                                   kwargs["dag"].dag_id,
                                   prev_task_id)
    source_full_path = os.path.join(source_dir_path, source_file_name)

    # download from GCS
    gcs_src_object = os.path.join(kwargs["dag"].dag_id, prev_task_id, source_file_name)
    gcs.upload(GCS_BUCKET, 
            gcs_src_object, 
            source_full_path, 
            mime_type='application/octet-stream')
    logging.info("Successfully download file from GCS: gs://{}/{}".format(GCS_BUCKET, gcs_src_object))

    # open the csv source file and read it in
    with open(source_full_path, 'r') as inputfile:
        csv_reader = csv.reader(inputfile, delimiter=',')
        for row in csv_reader:
            insert_cmd = """INSERT INTO weather 
                            (city, country, latitude, longitude,
                            todays_date, humidity, pressure, 
                            min_temp, max_temp, temp, weather)
                            VALUES
                            (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""

            pg_hook.run(insert_cmd, parameters=row)
            logging.info("Successfully insert to database using command: {}".format(insert_cmd))


if __name__ == "__main__":
    load_table()
