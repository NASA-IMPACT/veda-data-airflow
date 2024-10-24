from datetime import timedelta
import json
import uuid

from airflow.models.variable import Variable
from airflow.models.xcom import LazyXComAccess
from airflow.decorators import task
from veda_data_pipeline.utils.s3_discovery import (
    s3_discovery_handler, EmptyFileListError
)

group_kwgs = {"group_id": "Discover", "tooltip": "Discover"}


@task(retries=1, retry_delay=timedelta(minutes=1))
def discover_from_s3_task(ti=None, event={}, **kwargs):
    """Discover grouped assets/files from S3 in batches of 2800. Produce a list of such files stored on S3 to process.
    This task is used as part of the discover_group subdag and outputs data to EVENT_BUCKET.
    """
    config = {
        **event,
        **ti.dag_run.conf,
    }
    # TODO test that this context var is available in taskflow
    last_successful_execution = kwargs.get("prev_start_date_success")
    if event.get("schedule") and last_successful_execution:
        config["last_successful_execution"] = last_successful_execution.isoformat()
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
