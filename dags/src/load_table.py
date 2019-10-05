import os
from airflow.hooks.postgres_hook import PostgresHook
import numpy as np
import csv


def load_table():
    """
    Processes the json data, checks the types and enters into the
    Postgres database.
    """

    pg_hook = PostgresHook(postgres_conn_id='weather_id')

    # Read file from previous task
    source_file_name = str(os.environ['AIRFLOW_CTX_EXECUTION_DATE']) + '.csv'
    source_dir_path = os.path.join(os.path.dirname(__file__),
                                   '..', '..',
                                   'data',
                                   os.environ['AIRFLOW_CTX_DAG_ID'],
                                   'transform_data')
    source_full_path = os.path.join(source_dir_path, source_file_name)

    # open the csv source file and read it in
    with open(source_full_path, 'r') as inputfile:
        csv_reader = csv.reader(inputfile, delimiter=',')
        for row in csv_reader:
            insert_cmd = """INSERT INTO weather_table 
                            (city, country, latitude, longitude,
                            todays_date, humidity, pressure, 
                            min_temp, max_temp, temp, weather)
                            VALUES
                            (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""

            pg_hook.run(insert_cmd, parameters=row)


if __name__ == "__main__":
    load_table()
