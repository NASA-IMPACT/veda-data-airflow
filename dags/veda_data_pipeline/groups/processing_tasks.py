from datetime import timedelta
import json
import logging
from copy import deepcopy
import smart_open
from airflow.models.variable import Variable
from airflow.decorators import task
from veda_data_pipeline.utils.submit_stac import submission_handler

group_kwgs = {"group_id": "Process", "tooltip": "Process"}


def log_task(text: str):
    logging.info(text)

@task
def extract_discovery_items_from_payload(ti, payload=None, **kwargs):
    discovery_items = ti.dag_run.conf.get("discovery_items") if not payload else payload.get("discovery_items")
    return discovery_items

@task
def remove_thumbnail_asset(ti):
    payload = deepcopy(ti.dag_run.conf)
    assets = payload.get("assets", {})
    if assets.get("thumbnail"):
        assets.pop("thumbnail")
    # if thumbnail was only asset, delete assets
    if not assets:
        payload.pop("assets")
    return payload

# with exponential backoff enabled, retry delay is converted to seconds
@task(retries=2, retry_delay=60, retry_exponential_backoff=True, max_active_tis_per_dag=5)
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


@task(max_active_tis_per_dag=5)
def build_stac_task(payload):
    from veda_data_pipeline.utils.build_stac.handler import stac_handler
    airflow_vars_json = Variable.get("aws_dags_variables", deserialize_json=True)
    event_bucket = airflow_vars_json.get("EVENT_BUCKET")
    return stac_handler(payload_src=payload, bucket_output=event_bucket)
