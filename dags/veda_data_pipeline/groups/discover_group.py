from datetime import timedelta
import uuid

from airflow.sdk import Variable
from airflow.decorators import task
from veda_data_pipeline.utils.s3_discovery import (
    s3_discovery_handler, EmptyFileListError
)
from deprecated import deprecated

group_kwgs = {"group_id": "Discover", "tooltip": "Discover"}


@task(retries=1, retry_delay=timedelta(minutes=1))
def discover_from_s3_task(event: dict={}, dag_run=None, payload: dict={}, prev_start_date_success: str=None):
    """Discover grouped assets/files from S3 in batches of 2800. Produce a list of such files stored on S3 to process.
    This task is used as part of the discover_group subdag and outputs data to EVENT_BUCKET.
    """

    payload = payload or dag_run.conf
    config = {
        **event,
        **payload,
    }

    if event.get("schedule") and prev_start_date_success:
        config["last_successful_execution"] = prev_start_date_success.isoformat()
    # (event, chunk_size=2800, role_arn=None, bucket_output=None):

    event_bucket = Variable.get("EVENT_BUCKET")
    read_assume_arn = Variable.get("ASSUME_ROLE_READ_ARN")
    
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
def get_files_task(payload, dag_run=None):
    """
    Get files from S3 produced by discovery or dataset tasks.
    Handles both single payload and multiple payload scenarios.
    """
    dag_run_id = dag_run.run_id
    results = []

    # Handle multiple payloads (dataset and items case)
    payloads = payload if isinstance(payload, list) else [payload]

    for item in payloads:
        # A dynamically-mapped upstream returns a lazy XCom sequence, not a dict
        if not isinstance(item, dict):
            item = item[0]
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
def get_files_to_process(payload, dag_run=None):
    """Get files from S3 produced by the discovery task.
    Used as part of both the parallel_run_process_rasters and parallel_run_process_vectors tasks.
    """
    if not isinstance(payload, dict):  # dynamic task mapping returns a lazy XCom sequence
        payload = payload[0]
    payloads_xcom = payload.pop("payload", [])
    dag_run_id = dag_run.run_id
    return [{
        "run_id": f"{dag_run_id}_{uuid.uuid4()}_{indx}",
        **payload,
        "payload": payload_xcom,
    } for indx, payload_xcom in enumerate(payloads_xcom)]


@task
@deprecated(reason="Please use get_files_task airflow task instead. This will be removed in the new release")
def get_dataset_files_to_process(payload, dag_run=None):
    """Get files from S3 produced by the dataset task.
    This is different from the get_files_to_process task as it produces a combined structure from repeated mappings.
    """
    dag_run_id = dag_run.run_id

    result = []
    for x in payload:
        if not isinstance(x, dict):  # dynamic task mapping returns a lazy XCom sequence
            x = x[0]
        payloads_xcom = x.pop("payload", [])
        payload_0 = x
        for indx, payload_xcom in enumerate(payloads_xcom):
            result.append({
                "run_id": f"{dag_run_id}_{uuid.uuid4()}_{indx}",
                **payload_0,
                "payload": payload_xcom,
            })
    return result
