from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
import logging
from airflow.sdk import task
from common.config import DATASET, SCHEMA_RAW
from common.sql_quries import PROC_DWH_TO_DM, PROC_RAW_TO_DWH, LOAD_BULK, upload_files
from common.config import SCHEMA_UTILS

default_args = {
    'owner': 'airflow',
    'conn_id': 'snowflake_default',
}

logger = logging.getLogger(__name__)

def upload_file_to_stage():
    file_path = DATASET
    hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
    sql = upload_files(file_path)
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
    def upload_to_stage():
        upload_file_to_stage()


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

    upload_to_stage() >> task_load_bulk >> task_proc_dwh >> task_proc_dm
