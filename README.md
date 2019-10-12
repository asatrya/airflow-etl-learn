# Simple ETL Using Airflow

This is a simple ETL using Airflow. First, we fetch data from API (extract). Then, we drop unused columns, convert to CSV, and validate (transform). Finally, we load the transformed data to database (load).

## Prerequisite

### Set Airflow Home Directory (for local run)

``` bash
export AIRFLOW_HOME=$PWD
```

### Set VirtualEnv (for local run)

``` bash
virtualenv --no-site-packages venv
```

### Install Dependency (for local run)

``` bash
sudo apt install libpq-dev python-dev

pip install -r dags/requirements.txt
```

### Setup PostgreSQL

Install PostgreSQL

``` bash
sudo apt-get update

sudo apt-get install postgresql postgresql-contrib
```

Create user and database

``` bash
# Create a new user called testuser
sudo -u postgres createuser --login --pwprompt testuser
Enter password for new role: xxxx

# Create a new database called testdb, owned by testuser.
$ sudo -u postgres createdb --owner=testuser testdb
```

Create table

``` bash
# Login to PostgreSQL: psql -U user database
$ psql -U <your-db-user> -d <your-db-name> -h <your-db-host>

# Create table
testdb=> CREATE TABLE IF NOT EXISTS weather
                (
                    city         TEXT,
                    country      TEXT,
                    latitude     REAL,
                    longitude    REAL,
                    todays_date  DATE,
                    humidity     REAL,
                    pressure     REAL,
                    min_temp     REAL,
                    max_temp     REAL,
                    temp         REAL,
                    weather      TEXT
                )
CREATE TABLE
```

### Register DB Connection to Airflow

1. In Airflow UI, select menu **Admin** > **Connections**
1. Select tab **Create**
1. Fill database credentials, for example:

   

   ``` txt
   Conn Id = weatherdb_postgres_conn
   Conn Type = PostgreSQL
   Host = <your-db-host>
   Schema = <your-db-name>
   Login = <your-db-user>
   Password = <your-db-password>
   Port = 5432
   ```

### Register GCP Connection to Airflow

We will use Google Cloud Storage to save tasks' output file (also as the source for succeeding tasks).

So you need to register GCP connection although you run on your local machine.

Follow these steps:

1. Create new Service Account in GCP IAM. Make sure it is assigned by a Role that has permission to read and write GCS bucket.

1. Create Key and download it as JSON file. Keep this file as you cannot download it after this time.

1. In Airflow UI, go to Admin --> Connections menu, and create a new connection.

1. Fill these value on create connection form:

   ``` txt
   Conn Id = gcp_airflow_lab
   Conn Type = Google Cloud Platform
   Project Id = <your-gcp-project-id>
   Keyfile path: left empty
   Keyfile JSON: copy-paste the content of JSON keyfile that you downloaded before
   Scopes = https://www.googleapis.com/auth/devstorage.read_write  
   ```

1. Click Save

### Register Open Weather API Key as Airflow Variables

1. Register a free account on https://openweathermap.org/api and get the API key.

1. On Airflow UI, go to Admin --> Variables menu, and create new Variable

1. Fill with these value:

   ```txt
   key = OPEN_WEATHER_API_KEY
   value = <your-api-key>
   ```

1. Click Save

### Register GCS Bucket

We will use Google Cloud Storage to save tasks' output file (also as the source for succeeding tasks).

1. Create a new bucket on GCS and give it a name

1. On Airflow UI, go to Admin --> Variables menu, and create new Variable

1. Fill with these value:

   ```txt
   key = GCS_BUCKET
   value = <your-gcs-bucket-name>
   ```

1. Click Save

## Test Running the DAG (local run)

We create a DAG that has 3 Tasks:

* `get_weather` 
* `transform_data` 
* `load_table` 

To test each Task:

``` bash
airflow test weatherDag get_weather 2019-10-05
airflow test weatherDag transform_data 2019-10-05
airflow test weatherDag load_table 2019-10-05
```

Backfilling:

``` bash
airflow backfill weatherDag -s 2019-10-01 -e 2019-10-05
```

## Deploy DAG to Cloud Composer

1. Create a Composer environment

1. Upload DAG files to corresponding DAG folder in GCS. You can upload it manually or use `gsutil` with this command (assuming you are in the project's root folder)

   ```bash
   gsutil rsync -r -x ".*\.pyc$" dags/  gs://<your-composer-environment-bucket>/dags
   ```

   Make sure this directory structure is uploaded to GCS bucket:

   ```txt
   dags/
      weatherDag.py
      src/
         __init__.py
         get_weather.py
         transform_data.py
         load_table.py
   ```

1. Check Airflow UI to see your new deployed DAG.

## Notes for Development Purposes

### Clear Task Instances via CLI

In local machine

``` bash
airflow clear <your-dag-id>
```

In Cloud Composer

```bash
gcloud composer environments run <your-composer-environment-name> --location=<your-composer-environment-location> clear -- <your-dag-id> -c
```

In general, to run Airflow CLI in Cloud Composer:

```bash
gcloud composer environments run <your-composer-environment-name> --location=<your-composer-environment-location> <airflow-subcommand> -- <parameters-and-options>
```
