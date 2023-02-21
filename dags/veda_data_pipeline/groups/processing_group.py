import json
import logging
import os

from airflow.utils.task_group import TaskGroup
from airflow.providers.amazon.aws.operators.ecs import EcsRunTaskOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.models.variable import Variable
import smart_open

from veda_data_pipeline.groups.ecs_tasks import subdag_ecs_task
from veda_data_pipeline.veda_pipeline_tasks.submit_stac.handler import (
    submission_handler,
)
from veda_data_pipeline.veda_pipeline_tasks.cogify.handler import cogify_handler


def load_processing_env():
    MWAA_STACK_CONF = Variable.get("MWAA_STACK_CONF", deserialize_json=True)
    os.environ["ASSUME_ROLE_ARN"] = Variable.get("ASSUME_ROLE_READ_ARN")
    os.environ["EVENT_BUCKET"] = MWAA_STACK_CONF["EVENT_BUCKET"]
    os.environ["COGNITO_APP_SECRET"] = Variable.get("COGNITO_APP_SECRET")


group_kwgs = {"group_id": "Process", "tooltip": "Process"}
subgroup_kwgs = {"ecs_group_id": "ECSTasks"}


def log_task(text: str):
    logging.info(text)


def submit_to_stac_ingestor_task(ti):
    print("Submit STAC ingestor")
    load_processing_env()
    event = json.loads(
        ti.xcom_pull(
            task_ids=f"{group_kwgs['group_id']}.{subgroup_kwgs['ecs_group_id']}.build_stac"
        )
    )

    success_file = event["payload"]["success_event_key"]
    with smart_open.open(success_file, "r") as _file:
        stac_items = json.loads(_file.read())

    for item in stac_items:
        submission_handler(item)
    return event


def cogify_task(ti):
    payload = ti.dag_run.conf
    load_processing_env()
    return cogify_handler(payload)


def cogify_choice(ti, **kwargs):
    # Only get the payload from the successful task
    payload = ti.dag_run.conf
    if payload["cogify"]:
        return f"{group_kwgs['group_id']}.cogify"

    return f"{group_kwgs['group_id']}.{subgroup_kwgs['ecs_group_id']}.build_stac_task_register"


def subdag_process():
    with TaskGroup(**group_kwgs) as process_grp:
        MWAA_STACK_CONF = Variable.get("MWAA_STACK_CONF", deserialize_json=True)
        acct_id = MWAA_STACK_CONF.get("ACCOUNT_ID")
        region = MWAA_STACK_CONF.get("AWS_REGION")
        prefix = MWAA_STACK_CONF.get("PREFIX")
        cogify_branching = BranchPythonOperator(
            task_id="cogify_branching",
            trigger_rule="one_success",
            python_callable=cogify_choice,
        )
        external_role_arn = Variable.get("ASSUME_ROLE_READ_ARN")
        build_stac = subdag_ecs_task(
            task_id="build_stac",
            task_definition_family="ecs_family",
            cpu="1024",
            memory="2048",
            cmd=[
                "/usr/local/bin/python",
                "handler.py",
                "--payload",
                "{}".format("{{ task_instance.dag_run.conf }}"),
            ],
            container_name=f"{prefix}-veda-stac-build",
            docker_image=f"{acct_id}.dkr.ecr.{region}.amazonaws.com/{prefix}-veda-build_stac",
            mwaa_stack_conf=MWAA_STACK_CONF,
            stage=MWAA_STACK_CONF.get("STAGE"),
            environment_vars=[
                {
                    "name": "EXTERNAL_ROLE_ARN",
                    "value": external_role_arn,
                },
                {
                    "name": "BUCKET",
                    "value": "veda-data-pipelines-staging-lambda-ndjson-bucket",
                },
                {
                    "name": "EVENT_BUCKET",
                    "value": MWAA_STACK_CONF.get("EVENT_BUCKET"),
                },
            ],
        )
        cogify = PythonOperator(task_id="cogify", python_callable=cogify_task)
        submit_to_stac_ingestor = PythonOperator(
            task_id="submit_to_stac_ingestor",
            python_callable=submit_to_stac_ingestor_task,
        )
        cogify_branching >> build_stac >> submit_to_stac_ingestor
        cogify_branching >> cogify >> build_stac >> submit_to_stac_ingestor
        return process_grp
