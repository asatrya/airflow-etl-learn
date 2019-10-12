# Simple ETL Using Airflow

This is a simple ETL using Airflow. First, we fetch data from API (extract). Then, we drop unused columns, convert to CSV, and validate (transform). Finally, we load the transformed data to database (load).

## Prerequisite

### Set Airflow Home Directory

```bash
export AIRFLOW_HOME=$PWD
```

### Set VirtualEnv

``` bash
virtualenv --no-site-packages venv
```

### Install Dependency

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
$ psql -U testuser -d testdb -h localhost
Password for user testuser: ......

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

   ```txt
   Conn Id = weatherdb_postgres_conn
   Conn Type = PostgreSQL
   Host = localhost
   Schema = testdb
   Login = testuser
   Password = password
   Port = 5432
   ```

## Running Script

We create a DAG that has 3 Tasks:

* `get_weather`
* `transform_data`
* `load_table`

To test each Task:

```bash
airflow test weatherDag get_weather 2019-10-05
airflow test weatherDag transform_data 2019-10-05
airflow test weatherDag load_table 2019-10-05
```

Backfilling:

```bash
airflow backfill weatherDag -s 2019-10-01 -e 2019-10-05
```

## Debug

Delete all task instance:

```bash
airflow clear weatherDag
```