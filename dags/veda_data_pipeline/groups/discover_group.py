import time
import uuid

from airflow.models.variable import Variable
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from airflow_multi_dagrun.operators import TriggerMultiDagRunOperator
from veda_data_pipeline.utils.s3_discovery import (
    s3_discovery_handler,
)

group_kwgs = {"group_id": "Discover", "tooltip": "Discover"}


def discover_from_s3_task(ti):
    """Discover grouped assets/files from S3 in batches of 2800. Produce a list of such files stored on S3 to process.
    This task is used as part of the discover_group subdag and outputs data to EVENT_BUCKET.
    """
    config = ti.dag_run.conf
    # (event, chunk_size=2800, role_arn=None, bucket_output=None):
    MWAA_STAC_CONF = Variable.get("MWAA_STACK_CONF", deserialize_json=True)
    read_assume_arn = Variable.get("ASSUME_ROLE_READ_ARN", default_var=None)
    return s3_discovery_handler(
        event=config,
        role_arn=read_assume_arn,
        bucket_output=MWAA_STAC_CONF["EVENT_BUCKET"],
    )


def get_files_to_process(ti):
    """Get files from S3 produced by the discovery task.
    Used as part of both the parallel_run_process_rasters and parallel_run_process_vectors tasks.
    """
    payload = ti.xcom_pull(task_ids=f"{group_kwgs['group_id']}.discover_from_s3")
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

    if payload.get("vector"):
        return f"{group_kwgs['group_id']}.parallel_run_process_vectors"
    return f"{group_kwgs['group_id']}.parallel_run_process_rasters"


def subdag_discover():
    with TaskGroup(**group_kwgs) as discover_grp:
        discover_from_s3 = PythonOperator(
            task_id="discover_from_s3",
            python_callable=discover_from_s3_task,
            op_kwargs={"text": "Discover from S3"},
        )

        raster_vector_branching = BranchPythonOperator(
            task_id="raster_vector_branching",
            trigger_rule=TriggerRule.ONE_SUCCESS,
            python_callable=vector_raster_choice,
        )

        run_process_raster = TriggerMultiDagRunOperator(
            task_id="parallel_run_process_rasters",
            trigger_dag_id="veda_ingest_raster",
            trigger_rule=TriggerRule.ONE_SUCCESS,
            python_callable=get_files_to_process,
        )

        run_process_vector = TriggerMultiDagRunOperator(
            task_id="parallel_run_process_vectors",
            trigger_dag_id="veda_ingest_vector",
            trigger_rule=TriggerRule.ONE_SUCCESS,
            python_callable=get_files_to_process,
        )

        (
            discover_from_s3
            >> raster_vector_branching
            >> [run_process_raster, run_process_vector]
        )
        return discover_grp
