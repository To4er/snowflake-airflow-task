from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
from airflow.sdk import task
from common.config import AIRLINE_DATA_PATH
from common.sql_quries import PROC_DWH_TO_DM, PROC_RAW_TO_DWH, LOAD_BULK, UPLOAD_FILES
from common.config import SCHEMA_UTILS
from common.utils import logger

default_args = {
    'owner': 'airflow',
    'conn_id': 'snowflake_default',
}

def upload_file_to_stage_func(file_path: str):
    file_path = file_path
    hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
    sql = UPLOAD_FILES.format(file_path=file_path)
    hook.run(sql)
    hook.run(sql)
    logger.info('File upload complete')

with DAG(
        dag_id='snowflake_main_pipeline',
        default_args=default_args,
        schedule=None,
        start_date=datetime(2024, 1, 1),
        catchup=False
) as dag:

    # Uploading files in stage
    @task
    def upload_file_to_stage_task():
        upload_file_to_stage_func(AIRLINE_DATA_PATH)


    # Task 1: Stage -> Raw
    task_load_bulk = SQLExecuteQueryOperator(
        task_id='load_bulk_file',
        sql=LOAD_BULK
    )

    # Task 2: Raw -> DWH
    task_proc_dwh = SQLExecuteQueryOperator(
        task_id='process_to_dwh',
        sql=PROC_RAW_TO_DWH,
        autocommit=True,
        hook_params={"schema": SCHEMA_UTILS}
    )

    # Task 3: DWH -> DM
    task_proc_dm = SQLExecuteQueryOperator(
        task_id='process_to_dm',
        sql=PROC_DWH_TO_DM,
        autocommit=True,
        hook_params= {"schema": SCHEMA_UTILS}
    )

    upload_file_to_stage_task() >> task_load_bulk >> task_proc_dwh >> task_proc_dm
