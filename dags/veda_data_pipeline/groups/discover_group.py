import time
import uuid

from airflow.models.variable import Variable
from airflow.models.xcom import LazyXComAccess
from airflow.operators.dummy_operator import DummyOperator as EmptyOperator
from airflow.decorators import task_group, task
from airflow.models.baseoperator import chain
from airflow.operators.python import BranchPythonOperator, PythonOperator, ShortCircuitOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.amazon.aws.operators.ecs import EcsRunTaskOperator
from veda_data_pipeline.utils.s3_discovery import (
    s3_discovery_handler, EmptyFileListError
)
from veda_data_pipeline.groups.processing_tasks import build_stac_kwargs, submit_to_stac_ingestor_task


group_kwgs = {"group_id": "Discover", "tooltip": "Discover"}

@task
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
    MWAA_STAC_CONF = Variable.get("MWAA_STACK_CONF", deserialize_json=True)
    read_assume_arn = Variable.get("ASSUME_ROLE_READ_ARN", default_var=None)
    # Making the chunk size small, this helped us process large data faster than
    # passing a large chunk of 500
    chunk_size = config.get("chunk_size", 500)
    try:
        return s3_discovery_handler(
            event=config,
            role_arn=read_assume_arn,
            bucket_output=MWAA_STAC_CONF["EVENT_BUCKET"],
            chunk_size=chunk_size
        )
    except EmptyFileListError as ex:
        print(f"Received an exception {ex}")
        # TODO replace short circuit operator behavior
        return {}

@task
def get_files_to_process(payload, ti=None):
    """Get files from S3 produced by the discovery task.
    Used as part of both the parallel_run_process_rasters and parallel_run_process_vectors tasks.
    """
    if isinstance(payload, LazyXComAccess): # if used as part of a dynamic task mapping
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


# this task group is defined for reference, but can not be used in expanded taskgroup maps
@task_group
def subdag_discover(event={}):
    # Define operators for non-taskflow tasks
    discover_from_s3 = discover_from_s3_task(event=event)

    submit_to_stac_ingestor = PythonOperator(
        task_id="submit_to_stac_ingestor",
        python_callable=submit_to_stac_ingestor_task,
    )
    
    # define DAG using taskflow notation
    discover_from_s3 = discover_from_s3_task(event=event)
    get_files = get_files_to_process()

    chain(discover_from_s3, get_files)

    build_stac_kwargs_task = build_stac_kwargs.expand(event=get_files)
    build_stac = EcsRunTaskOperator.partial(
        task_id="build_stac"
    ).expand_kwargs(build_stac_kwargs_task)

    submit_to_stac_ingestor.expand(build_stac)
    


    
    
