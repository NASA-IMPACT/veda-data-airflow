from datetime import timedelta, datetime, timezone
import json
import logging
from copy import deepcopy
import smart_open
from airflow.sdk import Variable
from airflow.decorators import task
from airflow.sdk import Asset, AssetAlias, Metadata
from veda_data_pipeline.utils.submit_stac import submission_handler

group_kwgs = {"group_id": "Process", "tooltip": "Process"}

def log_task(text: str):
    logging.info(text)

@task
def extract_discovery_items_from_payload(payload=None, dag_run=None, **kwargs):
    discovery_items = dag_run.conf.get("discovery_items") if not payload else payload.get("discovery_items")
    return discovery_items

@task
def remove_thumbnail_asset(dag_run=None):
    payload = deepcopy(dag_run.conf)
    assets = payload.get("assets", {})
    if assets.get("thumbnail"):
        assets.pop("thumbnail")
    # if thumbnail was only asset, delete assets
    if not assets:
        # Safely return if there are no assets in the payload
        payload.pop("assets", True)
    return payload

# with exponential backoff enabled, retry delay is converted to seconds
@task(retries=2, retry_delay=60, retry_exponential_backoff=True, max_active_tis_per_dag=5)
def submit_to_stac_ingestor_task(built_stac: dict):
    """Submit STAC items to the STAC ingestor API."""
    event = built_stac.copy()
    success_file = event["payload"]["success_event_key"]

    app_secret = Variable.get("aws_dags_variables", deserialize_json=True).get("INGEST_API_KEYCLOAK_APP_SECRET")
    stac_ingestor_api_url = Variable.get("STAC_INGESTOR_API_URL")
    try:
        success_file = event["payload"]["success_event_key"]
        with smart_open.open(success_file, "r") as _file:
            stac_items = json.loads(_file.read())
    except KeyError:
        log_task("No success file found - using event directly")
        stac_items = [event]

    for item in stac_items:
        submission_handler(
            event=item,
            endpoint="/ingestions",
            app_secret=app_secret,
            stac_ingestor_api_url=stac_ingestor_api_url,
        )
    return event

@task(retries=2, retry_delay=60, retry_exponential_backoff=True, max_active_tis_per_dag=5)
def submit_to_stac_ingestor_task_direct(stac_items: dict):
    # to submit items without a success file
    app_secret = Variable.get("aws_dags_variables", deserialize_json=True).get("INGEST_API_KEYCLOAK_APP_SECRET")
    stac_ingestor_api_url = Variable.get("STAC_INGESTOR_API_URL")

    submission_handler(
        event=stac_items,
        endpoint="/ingestions",
        app_secret=app_secret,
        stac_ingestor_api_url=stac_ingestor_api_url,
    )
    return


@task(max_active_tis_per_dag=5)
def build_stac_task(payload, ti=None):
    from veda_data_pipeline.utils.build_stac.handler import stac_handler
    event_bucket = Variable.get("EVENT_BUCKET")
    return stac_handler(payload_src=payload, bucket_output=event_bucket, ti=ti)

@task(
        outlets=[
            AssetAlias("VEDA-Datasets")
        ],
)
def post_ingest_dataset_event(logical_date=None, built_items = {}, dag_run=None):  # params are Airflow kwargs - use this task without input
    """
    Logs a Dataset event, saving the config used as a versioned object in s3, and creating a Metadata object visible in Airflow.

    Datasets are per-collection, with an alias of "VEDA-Datasets" for additional DAG triggers.

    Args:
        (Automatically populated by airflow when invoked)
        dag_run: Airflow DagRun, used to access the DAG run configuration.
        logical_date: The logical date of the DAG run, used for versioning.
    Returns:
        Yields a Metadata object that Airflow uses to register the Dataset event.
    """
    payload = dag_run.conf
    event_bucket_name = Variable.get("EVENT_BUCKET")
    collection = payload.get("collection", None)
    if not collection:
        raise ValueError("Collection ID is required in the payload to create a report.")

    event_dt = (
        logical_date
        or getattr(dag_run, "logical_date", None)
        or getattr(dag_run, "run_after", None)
        or getattr(dag_run, "start_date", None)
        or datetime.now(timezone.utc)
    )

    # write the payload to S3 as a versioned object
    key = f"s3://{event_bucket_name}/airflow_events/{collection}/{event_dt.strftime('%Y%m%d%H%M%S')}.json"
    try:
        with smart_open.open(key, "w") as f:
            json.dump(payload, f, indent=2)
    except Exception as e:
        log_task(f"Error writing payload to {key}: {e}")
        raise
    log_task(f"Payload written to {key}")

    # built items can be either a dict or a list of dicts
    if isinstance(built_items, dict):
        built_items = [built_items]
    elif not isinstance(built_items, list):
        built_items = list(built_items)
    print(f"Built items: {built_items}")
    success_count = sum(item.get("payload", {}).get("status", {}).get("successes", 0) for item in built_items)
    failure_count = sum(item.get("payload", {}).get("status", {}).get("failures", 0) for item in built_items)

    yield Metadata(
        Asset(f"{collection}"),
        extra={
            "ingest_datetime": str(event_dt),
            "ingest_configuration": key,
            "successful_items": success_count,
            "failed_items": failure_count,
        },  # extra has to be provided, can be {}
        alias=AssetAlias("VEDA-Datasets"),
    )
