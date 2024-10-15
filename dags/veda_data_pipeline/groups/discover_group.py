import json
import time
import uuid

from airflow.decorators import task_group
from airflow.models.variable import Variable
from airflow.models.xcom import LazyXComAccess
from airflow.operators.dummy_operator import DummyOperator as EmptyOperator
from airflow.operators.python import (BranchPythonOperator, PythonOperator,
                                      ShortCircuitOperator)
from airflow.utils.trigger_rule import TriggerRule
from airflow_multi_dagrun.operators import TriggerMultiDagRunOperator

group_kwgs = {"group_id": "Discover", "tooltip": "Discover"}


def discover_from_s3_task(ti, event={}, **kwargs):
    """Discover grouped assets/files from S3 in batches of 2800. Produce a list of such files stored on S3 to process.
    This task is used as part of the discover_group subdag and outputs data to EVENT_BUCKET.
    """
    from veda_data_pipeline.utils.s3_discovery import (EmptyFileListError,
                                                       s3_discovery_handler)
    config = {
        **event,
        **ti.dag_run.conf,
    }
    last_successful_execution = kwargs.get("prev_start_date_success")
    if event.get("schedule") and last_successful_execution:
        config["last_successful_execution"] = last_successful_execution.isoformat()
    # (event, chunk_size=2800, role_arn=None, bucket_output=None):
    airflow_vars = Variable.get("aws_dags_variables")
    airflow_vars_json = json.loads(airflow_vars)
    event_bucket = airflow_vars_json.get("EVENT_BUCKET")
    read_assume_arn = Variable.get("ASSUME_ROLE_READ_ARN", default_var=None)
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
        return []


def get_files_to_process(ti):
    """Get files from S3 produced by the discovery task.
    Used as part of both the parallel_run_process_rasters and parallel_run_process_vectors tasks.
    """
    dynamic_group_id = ti.task_id.split(".")[0]
    payload = ti.xcom_pull(task_ids=f"{dynamic_group_id}.discover_from_s3")
    if isinstance(payload, LazyXComAccess):
        payloads_xcom = payload[0].pop("payload", [])
        payload = payload[0]
    else:
        payloads_xcom = payload.pop("payload", [])
    dag_run_id = ti.dag_run.run_id
    for indx, payload_xcom in enumerate(payloads_xcom):
        time.sleep(2)
        yield {
            "run_id": f"{dag_run_id}_{uuid.uuid4()}_{indx}",
            **payload,
            "payload": payload_xcom,
        }


def vector_raster_choice(ti):
    """Choose whether to process rasters or vectors based on the payload."""
    payload = ti.dag_run.conf
    dynamic_group_id = ti.task_id.split(".")[0]

    if payload.get("vector"):
        return f"{dynamic_group_id}.parallel_run_process_generic_vectors"
    if payload.get("vector_eis"):
        return f"{dynamic_group_id}.parallel_run_process_vectors"
    return f"{dynamic_group_id}.parallel_run_process_rasters"


@task_group
def subdag_discover(event=None):
    # Replace the mutable default parameter by None
    if event is None:
        event = dict()
    discover_from_s3 = ShortCircuitOperator(
        task_id="discover_from_s3",
        python_callable=discover_from_s3_task,
        op_kwargs={"text": "Discover from S3", "event": event},
        trigger_rule=TriggerRule.NONE_FAILED,
        provide_context=True,
    )

    raster_vector_branching = BranchPythonOperator(
        task_id="raster_vector_branching",
        python_callable=vector_raster_choice,
    )

    run_process_raster = TriggerMultiDagRunOperator(
        task_id="parallel_run_process_rasters",
        trigger_dag_id="veda_ingest_raster",
        python_callable=get_files_to_process,
    )

    run_process_vector = TriggerMultiDagRunOperator(
        task_id="parallel_run_process_vectors",
        trigger_dag_id="veda_ingest_vector",
        python_callable=get_files_to_process,
    )

    run_process_generic_vector = TriggerMultiDagRunOperator(
        task_id="parallel_run_process_generic_vectors",
        trigger_dag_id="veda_generic_ingest_vector",
        python_callable=get_files_to_process,
    )

    # extra no-op, needed to run in dynamic mapping context
    end_discover = EmptyOperator(task_id="end_discover", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS, )

    discover_from_s3 >> raster_vector_branching >> [run_process_raster, run_process_vector, run_process_generic_vector]
    run_process_raster >> end_discover
    run_process_vector >> end_discover
    run_process_generic_vector >> end_discover
