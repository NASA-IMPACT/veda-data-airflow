from datetime import timedelta
import json
import uuid

from airflow.models.variable import Variable
from airflow.models.xcom import LazyXComAccess
from airflow.decorators import task
from veda_data_pipeline.utils.s3_discovery import (
    s3_discovery_handler, EmptyFileListError
)
from deprecated import deprecated

group_kwgs = {"group_id": "Discover", "tooltip": "Discover"}


@task(retries=1, retry_delay=timedelta(minutes=1))
def discover_from_s3_task(*, event: dict = {}, ti=None, payload: dict ={}, prev_start_date_success: str = None):
    """Discover grouped assets/files from S3 in batches of 2800. Produce a list of such files stored on S3 to process.
    This task is used as part of the discover_group subdag and outputs data to EVENT_BUCKET.
    """

    payload = payload or ti.dag_run.conf
    config = {
        **event,
        **payload,
    }
    # TODO test that this context var is available in taskflow
    if event.get("schedule") and prev_start_date_success:
        config["last_successful_execution"] = prev_start_date_success.isoformat()
    # (event, chunk_size=2800, role_arn=None, bucket_output=None):

    airflow_vars = Variable.get("aws_dags_variables")
    airflow_vars_json = json.loads(airflow_vars)
    event_bucket = airflow_vars_json.get("EVENT_BUCKET")
    read_assume_arn = airflow_vars_json.get("ASSUME_ROLE_READ_ARN")
    # Making the chunk size small, this helped us process large data faster than
    # passing a large chunk of 500
    chunk_size = config.get("chunk_size", 500)
    try:
        return s3_discovery_handler(
            event=config,
            role_arn=read_assume_arn,
            bucket_output=event_bucket,
            chunk_size=chunk_size
        )
    except EmptyFileListError as ex:
        print(f"Received an exception {ex}")
        # TODO test continued short circuit operator behavior (no files -> skip remaining tasks)
        return {}


@task
def get_files_task(payload, ti=None):
    """
    Get files from S3 produced by discovery or dataset tasks.
    Handles both single payload and multiple payload scenarios.
    """
    dag_run_id = ti.dag_run.run_id
    results = []

    # Handle multiple payloads (dataset and items case)
    payloads = payload if isinstance(payload, list) else [payload]

    for item in payloads:
        if isinstance(item, LazyXComAccess):  # Dynamic task mapping case
            payloads_xcom = item[0].pop("payload", [])
            base_payload = item[0]
        else:
            payloads_xcom = item.pop("payload", [])
            base_payload = item

        for indx, payload_xcom in enumerate(payloads_xcom):
            results.append({
                "run_id": f"{dag_run_id}_{uuid.uuid4()}_{indx}",
                **base_payload,
                "payload": payload_xcom,
            })

    return results

@task
@deprecated(reason="Please use get_files_task function that handles both files and dataset files use cases")
def get_files_to_process(payload, ti=None):
    """Get files from S3 produced by the discovery task.
    Used as part of both the parallel_run_process_rasters and parallel_run_process_vectors tasks.
    """
    if isinstance(payload, LazyXComAccess):  # if used as part of a dynamic task mapping
        payloads_xcom = payload[0].pop("payload", [])
        payload = payload[0]
    else:
        payloads_xcom = payload.pop("payload", [])
    dag_run_id = ti.dag_run.run_id
    return [{
        "run_id": f"{dag_run_id}_{uuid.uuid4()}_{indx}",
        **payload,
        "payload": payload_xcom,
    } for indx, payload_xcom in enumerate(payloads_xcom)]


@task
@deprecated(reason="Please use get_files_task airflow task instead. This will be removed in the new release")
def get_dataset_files_to_process(payload, ti=None):
    """Get files from S3 produced by the dataset task.
    This is different from the get_files_to_process task as it produces a combined structure from repeated mappings.
    """
    dag_run_id = ti.dag_run.run_id

    result = []
    for x in payload:
        if isinstance(x, LazyXComAccess):  # if used as part of a dynamic task mapping
            payloads_xcom = x[0].pop("payload", [])
            payload_0 = x[0]
        else:
            payloads_xcom = x.pop("payload", [])
            payload_0 = x
        for indx, payload_xcom in enumerate(payloads_xcom):
            result.append({
                "run_id": f"{dag_run_id}_{uuid.uuid4()}_{indx}",
                **payload_0,
                "payload": payload_xcom,
            })
    return result
