import logging
import time

import pendulum
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator as EmptyOperator
from airflow.operators.python import PythonOperator


def log_task(text: str):
    logging.info(text)


def discover_from_cmr_task(text):
    log_task(text)


def discover_from_s3_task(text):
    log_task("I am discovering")
    time.sleep(1)
    log_task("Done discovering")
    log_task(text)


def move_files_to_maap_store_task(text):
    log_task("I am moving files")
    time.sleep(3)
    log_task("Done moving files")
    log_task(text)


def generate_cmr_metadata_task(text):
    log_task(text)


def push_to_cmr_task(text):
    log_task(text)


with DAG(
    dag_id="example_etl_flow_test",
    start_date=pendulum.today("UTC").add(days=-1),
    schedule_interval=None,
    tags=["example"],
) as dag:

    start = EmptyOperator(task_id="start", dag=dag)
    
    discover_from_cmr = PythonOperator(
        task_id="discover_from_cmr",
        python_callable=discover_from_cmr_task,
        op_kwargs={"text": "Discover from CMR"},
        dag=dag,
    )

    discover_from_s3 = PythonOperator(
        task_id="discover_from_s3",
        python_callable=discover_from_s3_task,
        op_kwargs={"text": "Discover from S3"},
        dag=dag,
    )

    move_files_to_maap_store = PythonOperator(
        task_id="move_files_to_maap_store",
        python_callable=move_files_to_maap_store_task,
        op_kwargs={"text": "Moving Files to MAAP store"},
        dag=dag,
    )

    generate_cmr_metadata = PythonOperator(
        task_id="generate_cmr_metadata",
        python_callable=generate_cmr_metadata_task,
        op_kwargs={"text": "Generate CMR metadata"},
        dag=dag,
    )

    push_to_cmr = PythonOperator(
        task_id="push_to_cmr",
        python_callable=push_to_cmr_task,
        op_kwargs={"text": "Push to CMR"},
        dag=dag,
    )

    end = EmptyOperator(task_id="end", dag=dag)

    start >> discover_from_cmr

    start >> discover_from_s3 >> move_files_to_maap_store
    (
        [discover_from_cmr, move_files_to_maap_store]
        >> generate_cmr_metadata
        >> push_to_cmr
        >> end
    )
