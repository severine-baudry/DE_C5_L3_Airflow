#Instructions
#In this exercise, we’ll refactor a DAG with a single overloaded task into a DAG with several tasks with well-defined boundaries
#1 - Read through the DAG and identify points in the DAG that could be split apart
#2 - Split the DAG into multiple PythonOperators
#3 - Run the DAG

import datetime
import logging

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook

from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator


#
# TODO: Finish refactoring this function into the appropriate set of tasks,
#       instead of keeping this one large task.
#

def log_oldest():
    redshift_hook = PostgresHook("redshift")
    records = redshift_hook.get_records("""
        SELECT birthyear FROM older_riders ORDER BY birthyear ASC LIMIT 1
    """)
    if len(records) > 0 and len(records[0]) > 0:
        logging.info(f"Oldest rider was born in {records[0][0]}")

def log_age_under_18():
     redshift_hook = PostgresHook("redshift")
     records = redshift_hook.get_records("""
        SELECT birthyear FROM younger_riders ORDER BY birthyear DESC LIMIT 1
     """)
     if len(records) > 0 and len(records[0]) > 0:
        logging.info(f"Youngest rider was born in {records[0][0]}")

def log_bike_ride_frequency():
    redshift_hook = PostgresHook("redshift")
    records = redshift_hook.get_records("""
        SELECT bikeid, count FROM lifetime_rides ORDER BY count DESCT LIMIT 5
    """)
 
dag = DAG(
    "lesson3.exercise2",
    start_date=datetime.datetime.utcnow()
)

#load_and_analyze = PythonOperator(
#    task_id='load_and_analyze',
#    dag=dag,
#    python_callable=load_and_analyze,
#    provide_context=True,
#)

create_oldest_task = PostgresOperator(
    task_id="create_oldest",
    dag=dag,
    sql="""
        BEGIN;
        DROP TABLE IF EXISTS older_riders;
        CREATE TABLE older_riders AS (
            SELECT * FROM trips WHERE birthyear > 0 AND birthyear <= 1945
        );
        COMMIT;
    """,
    postgres_conn_id="redshift"
)

log_oldest_task = PythonOperator(
    task_id="log_oldest",
    dag=dag,
    python_callable=log_oldest
)

    # Find all trips where the rider was under 18
create_age_under_18_task = PostgresOperator(
    task_id = "create_age_under_18",
    dag = dag,
    sql = """
        BEGIN;
        DROP TABLE IF EXISTS younger_riders;
        CREATE TABLE younger_riders AS (
            SELECT * FROM trips WHERE birthyear > 2000
        );
        COMMIT;
    """,
    postgres_conn_id="redshift"
)
log_age_under_18_task = PythonOperator(
    task_id="log_age_under_18",
    dag=dag,
    python_callable=log_age_under_18
)

create_bike_ride_frequency_task = PostgresOperator(
# Find out how often each bike is ridden
    task_id = "create_bike_ride_frequency",
    dag = dag,
    sql = """
        BEGIN;
        DROP TABLE IF EXISTS lifetime_rides;
        CREATE TABLE lifetime_rides AS (
            SELECT bikeid, COUNT(bikeid) AS count
            FROM trips
            GROUP BY bikeid
        );
        COMMIT;
    """,
    postgres_conn_id = "redshift"
)

log_bike_ride_frequency_task = PythonOperator(
    task_id = "log_bike_ride_frequency",
    dag = dag,
    python_callable = log_bike_ride_frequency

)

create_number_stations = PostgresOperator(
    # Count the number of stations by city
    task_id = "create_number_stations",
    sql = """
        BEGIN;
        DROP TABLE IF EXISTS city_station_counts;
        CREATE TABLE city_station_counts AS(
            SELECT city, COUNT(city)
            FROM stations
            GROUP BY city
        );
        COMMIT;
    """,
    postgres_conn_id = "redshift"
    )

create_oldest_task >> log_oldest_task
create_age_under_18_task >> log_age_under_18_task
create_bike_ride_frequency_task >> log_bike_ride_frequency_task
#create_bie_ride_frequency 