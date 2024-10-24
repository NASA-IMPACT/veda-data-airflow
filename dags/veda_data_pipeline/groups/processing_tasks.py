from datetime import timedelta
import json
import logging
import smart_open
from airflow.models.variable import Variable
from airflow.decorators import task
from veda_data_pipeline.utils.submit_stac import submission_handler

group_kwgs = {"group_id": "Process", "tooltip": "Process"}


def log_task(text: str):
    logging.info(text)


@task(retries=1, retry_delay=timedelta(minutes=1))
def submit_to_stac_ingestor_task(built_stac: dict):
    """Submit STAC items to the STAC ingestor API."""
    event = built_stac.copy()
    success_file = event["payload"]["success_event_key"]

    airflow_vars = Variable.get("aws_dags_variables")
    airflow_vars_json = json.loads(airflow_vars)
    cognito_app_secret = airflow_vars_json.get("COGNITO_APP_SECRET")
    stac_ingestor_api_url = airflow_vars_json.get("STAC_INGESTOR_API_URL")
    with smart_open.open(success_file, "r") as _file:
        stac_items = json.loads(_file.read())

    for item in stac_items:
        submission_handler(
            event=item,
            endpoint="/ingestions",
            cognito_app_secret=cognito_app_secret,
            stac_ingestor_api_url=stac_ingestor_api_url,
        )
    return event




