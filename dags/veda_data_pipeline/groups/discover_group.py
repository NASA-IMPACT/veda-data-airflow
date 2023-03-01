import time
import uuid

from airflow.models.variable import Variable
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from airflow_multi_dagrun.operators import TriggerMultiDagRunOperator
from veda_data_pipeline.veda_pipeline_tasks.s3_discovery.handler import \
    s3_discovery_handler

group_kwgs = {"group_id": "Discover", "tooltip": "Discover"}


def get_payload(ti_xcom_pull):
    task_ids = [
        f"{group_kwgs['group_id']}.discover_from_s3",
        f"{group_kwgs['group_id']}.discover_from_cmr",
    ]
    return [
        payload for payload in ti_xcom_pull(task_ids=task_ids) if payload is not None
    ][0]


def discover_from_cmr_task(text):
    return {"place_holder": text}


def discover_from_s3_task(ti):
    config = ti.dag_run.conf
    # (event, chunk_size=2800, role_arn=None, bucket_output=None):
    MWAA_STAC_CONF = Variable.get("MWAA_STACK_CONF", deserialize_json=True)
    read_assume_arn = Variable.get("ASSUME_ROLE_READ_ARN")
    return s3_discovery_handler(
        event=config,
        role_arn=read_assume_arn,
        bucket_output=MWAA_STAC_CONF["EVENT_BUCKET"],
    )


def get_files_to_process(ti):
    payload = get_payload(ti.xcom_pull)
    payloads_xcom = payload.pop("payload", [])
    dag_run_id = ti.dag_run.run_id
    for indx, payload_xcom in enumerate(payloads_xcom):
        time.sleep(2)
        yield {
            "run_id": f"{dag_run_id}_{uuid.uuid4()}_{indx}",
            **payload,
            "payload": payload_xcom,
        }


def discover_choice(ti):
    config = ti.dag_run.conf
    supported_discoveries = {"s3": "discover_from_s3", "cmr": "discover_from_cmr"}
    return f"{group_kwgs['group_id']}.{supported_discoveries[config['discovery']]}"


def subdag_discover():
    with TaskGroup(**group_kwgs) as discover_grp:
        discover_branching = BranchPythonOperator(
            task_id="discover_branching", python_callable=discover_choice
        )

        discover_from_cmr = PythonOperator(
            task_id="discover_from_cmr",
            python_callable=discover_from_cmr_task,
            op_kwargs={"text": "Discover from CMR"},
        )
        discover_from_s3 = PythonOperator(
            task_id="discover_from_s3",
            python_callable=discover_from_s3_task,
            op_kwargs={"text": "Discover from S3"},
        )
        run_process = TriggerMultiDagRunOperator(
            task_id="parallel_run_process_tasks",
            trigger_dag_id="veda_ingest",
            trigger_rule=TriggerRule.ONE_SUCCESS,
            python_callable=get_files_to_process,
        )

        discover_branching >> [discover_from_cmr, discover_from_s3] >> run_process
        return discover_grp
