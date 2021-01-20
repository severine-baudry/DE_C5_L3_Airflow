import datetime

from airflow import DAG

from airflow.operators import (
    FactsCalculatorOperator,
    HasRowsOperator,
    S3ToRedshiftOperator,
    DummySQLOperator,
    PostgresOperator
)

import sql_statements

my_redshift_conn = "redshift"
my_aws_credentials="aws_credentials"


#
# TODO: Create a DAG which performs the following functions:
#
#       1. Loads Trip data from S3 to RedShift
#       2. Performs a data quality check on the Trips table in RedShift
#       3. Uses the FactsCalculatorOperator to create a Facts table in Redshift
#           a. **NOTE**: to complete this step you must complete the FactsCalcuatorOperator
#              skeleton defined in plugins/operators/facts_calculator.py
#
dag = DAG("lesson3.exercise4", start_date=datetime.datetime.utcnow())

create_trips_table = PostgresOperator(
    task_id="create_trips_table",
    dag=dag,
    postgres_conn_id=my_redshift_conn,
    sql=sql_statements.CREATE_TRIPS_TABLE_SQL
)

#
# TODO: Load trips data from S3 to RedShift. Use the s3_key
#       "data-pipelines/divvy/unpartitioned/divvy_trips_2018.csv"
#       and the s3_bucket "udacity-dend"
#
copy_trips_task = S3ToRedshiftOperator(
                task_id="load_trips_from_s3_to_redshift",
                dag=dag,
                redshift_conn_id = my_redshift_conn,
                aws_credentials_id = my_aws_credentials,
                table="trips",
                s3_bucket="udacity-dend",
                s3_key="data-pipelines/divvy/unpartitioned/divvy_trips_2018.csv"
            )

#
# TODO: Perform a data quality check on the Trips table
#
check_trips = HasRowsOperator(
                 task_id="check_hasrows_trips",
                 dag=dag,
                 redshift_conn_id=my_redshift_conn,
                 table="trips"
                )

#
# TODO: Use the FactsCalculatorOperator to create a Facts table in RedShift. The fact column should
#       be `tripduration` and the groupby_column should be `bikeid`
#
calculate_facts = FactsCalculatorOperator(
                task_id = "calculate_facts_trips",
                dag = dag,
                redshift_conn_id = my_redshift_conn,
                origin_table="trips",
                destination_table="table_tripduration",
                fact_column="tripduration",
                groupby_column="bikeid"
        )

dummy_sql = DummySQLOperator(
                task_id = "dummy_sql",
                dag = dag,
                origin_table="trips",
                destination_table="table_tripduration",
                fact_column="tripduration",
                groupby_column="bikeid"
        )
#
# TODO: Define task ordering for the DAG tasks you defined
#
create_trips_table >> copy_trips_task >> check_trips >> calculate_facts