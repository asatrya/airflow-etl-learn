from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator


# Define the default dag arguments.
default_args = {
    'owner': 'Mike',
    'depends_on_past': False,
    'email': ['mdh266@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=1)
}


# Define the dag, the start date and how frequently it runs.
# I chose the dag to run everday by using 1440 minutes.
dag = DAG(
    dag_id='weatherDag',
    default_args=default_args,
    start_date=datetime(2017, 8, 24),
    schedule_interval=timedelta(minutes=1440))


# First task is to query get the weather from openweathermap.org.
task1 = BashOperator(
    task_id='get_weather',
    bash_command='python ~/airflow/dags/src/get_weather.py',
    dag=dag)


# Second task is to process the data and load into the database.
task2 = BashOperator(
    task_id='transform_data',
    bash_command='python ~/airflow/dags/src/transform_data.py',
    dag=dag)

# Second task is to process the data and load into the database.
task3 = BashOperator(
    task_id='load_table',
    bash_command='python ~/airflow/dags/src/load_table.py',
    dag=dag)

# Set task1 "upstream" of task2, i.e. task1 must be completed
# before task2 can be started.
task1 >> task2 >> task3