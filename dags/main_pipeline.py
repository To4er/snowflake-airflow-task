from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
import os

default_args = {
    'owner': 'airflow',
    'conn_id': 'snowflake_default',
}

with DAG(
        dag_id='snowflake_main_pipeline',
        default_args=default_args,
        schedule=None,
        start_date=datetime(2024, 1, 1),
        catchup=False
) as dag:

    task_load_bulk = SQLExecuteQueryOperator(
        task_id='1_load_bulk_file',
        sql="""
            COPY INTO AIRLINE_DB.RAW.PASSENGERS_RAW (
                PASSENGER_ID, FIRST_NAME, LAST_NAME, GENDER, AGE, 
                NATIONALITY, AIRPORT_NAME, AIRPORT_COUNTRY_CODE, COUNTRY_NAME, 
                AIRPORT_CONTINENT, CONTINENTS, DEPARTURE_DATE, ARRIVAL_AIRPORT, 
                PILOT_NAME, FLIGHT_STATUS, TICKET_TYPE, PASSENGER_STATUS
            )
            FROM (
                SELECT 
                    t.$2, t.$3, t.$4, t.$5, t.$6, t.$7, t.$8, t.$9, t.$10, 
                    t.$11, t.$12, t.$13, t.$14, t.$15, t.$16, t.$17, t.$18
                FROM @AIRLINE_DB.RAW.MY_INT_STAGE t
            )
            FILE_FORMAT = (
                TYPE = 'CSV' 
                SKIP_HEADER = 1 
                FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            )
            ON_ERROR = 'CONTINUE'
            FORCE = TRUE;
        """
    )

    # Task 2: Процедура RAW -> DWH
    task_proc_dwh = SQLExecuteQueryOperator(
        task_id='2_process_to_dwh',
        sql="CALL AIRLINE_DB.UTILS.PROC_RAW_TO_DWH();",
        autocommit=True
    )

    # Task 3: Процедура DWH -> DM
    task_proc_dm = SQLExecuteQueryOperator(
        task_id='3_process_to_dm',
        sql="CALL AIRLINE_DB.UTILS.PROC_DWH_TO_DM();",
        autocommit=True
    )

    # Порядок выполнения
    task_load_bulk >> task_proc_dwh >> task_proc_dm