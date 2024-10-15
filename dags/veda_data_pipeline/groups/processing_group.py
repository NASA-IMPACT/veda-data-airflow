import json
import logging
from datetime import timedelta

import smart_open
from airflow.models.variable import Variable
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

group_kwgs = {"group_id": "Process", "tooltip": "Process"}


def log_task(text: str):
    logging.info(text)


def submit_to_stac_ingestor_task(ti):
    """Submit STAC items to the STAC ingestor API."""
    from veda_data_pipeline.utils.submit_stac import submission_handler
    print("Submit STAC ingestor")
    event = ti.xcom_pull(task_ids=f"{group_kwgs['group_id']}.build_stac")
    success_file = event["payload"]["success_event_key"]
    with smart_open.open(success_file, "r") as _file:
        stac_items = json.loads(_file.read())
    airflow_vars = Variable.get("aws_dags_variables")
    airflow_vars_json = json.loads(airflow_vars)
    for item in stac_items:
        submission_handler(
            event=item,
            endpoint="/ingestions",
            cognito_app_secret=airflow_vars_json.get("COGNITO_APP_SECRET"),
            stac_ingestor_api_url=airflow_vars_json.get("STAC_INGESTOR_API_URL"),
        )
    return event


def build_stac_task(ti):
    from veda_data_pipeline.utils.build_stac import build_stac_handler
    config = ti.dag_run.conf
    airflow_vars = Variable.get("aws_dags_variables")
    airflow_vars_json = json.loads(airflow_vars)
    read_role_arn = airflow_vars_json.get("ASSUME_ROLE_READ_ARN")
    event_bucket = airflow_vars_json.get("EVENT_BUCKET")
    return build_stac_handler(config, bucket_output=event_bucket, role_arn=read_role_arn)


def subdag_process():
    with TaskGroup(**group_kwgs) as process_grp:
        build_stac = PythonOperator(
            task_id="build_stac",
            python_callable=build_stac_task,
        )
        submit_to_stac_ingestor = PythonOperator(
            task_id="submit_to_stac_ingestor",
            python_callable=submit_to_stac_ingestor_task,
        )
        build_stac >> submit_to_stac_ingestor
        return process_grp
