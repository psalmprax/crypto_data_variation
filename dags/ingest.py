# import psycopg2
import os
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator

from apicrypto import ApiCrypto
from config import Config
from utilsfile import set_logger, config_reader

cfg = Config()
PARENT_PATH = os.fspath(Path(__file__).parents[1])
CONFIG_PATH = os.path.join(
    PARENT_PATH,
    "configurations",
    "settings.ini")

FILE_TEMP_STORE = os.path.join(
    PARENT_PATH,
    "file_temp_store",
    "data.json")
SOURCE_URL = config_reader(CONFIG_PATH, "api")["url"]
CONN = config_reader(CONFIG_PATH, "api")["conn"]
TBLNAME = config_reader(CONFIG_PATH, "api")["tablename"]
TBLNAME_VARIATION = config_reader(CONFIG_PATH, "api")["tablename_variation"]

dag_params = {
    'dag_id': 'Ingest',
    'start_date': datetime(2021, 4, 19),
    'schedule_interval': None
}


def main(ds=None, **kwargs):
    """
    main function point of entry for execution
    :param ds:
    :type ds:
    :param kwargs:
    :type kwargs:
    """
    LOGGER = set_logger("main_logger")

    try:
        crypto_dt = ApiCrypto()
        dataset, status = crypto_dt.get_json_api(SOURCE_URL)
        crypto_dt.store_api_data(dataset, FILE_TEMP_STORE)
        crypto_dt.save_json(FILE_TEMP_STORE, TBLNAME,CONN)
        crypto_dt.load_save_db_api_data_variation(FILE_TEMP_STORE, TBLNAME_VARIATION, CONN)
    except Exception as e:
        LOGGER.info(f"Exception: {e}")


with DAG(**dag_params, template_searchpath=[cfg.dir_dag_template]) as dag:
    create_extension_task = PostgresOperator(
        task_id='create_extension',
        sql="create_extension.sql",
        postgres_conn_id="connections"
    )

    create_target_table = PostgresOperator(
        task_id='create_table',
        sql="create_target_tables.sql",
        postgres_conn_id="connections"
    )

    insert_target_table = PythonOperator(
        task_id='insert_target_table',
        python_callable=main,
        provide_context=True
    )

    create_extension_task >> create_target_table >> insert_target_table
