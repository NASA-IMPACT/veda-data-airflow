import json
import logging
from datetime import timedelta

import smart_open
from airflow.models.variable import Variable
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.amazon.aws.operators.ecs import EcsRunTaskOperator
from airflow.utils.task_group import TaskGroup
from veda_data_pipeline.veda_pipeline_tasks.cogify.handler import (
    cogify_handler,
)
from veda_data_pipeline.veda_pipeline_tasks.submit_stac.handler import (
    submission_handler,
)

group_kwgs = {"group_id": "Process", "tooltip": "Process"}


def log_task(text: str):
    logging.info(text)


def submit_to_stac_ingestor_task(ti):
    print("Submit STAC ingestor")
    event = json.loads(ti.xcom_pull(task_ids=f"{group_kwgs['group_id']}.build_stac"))
    success_file = event["payload"]["success_event_key"]
    with smart_open.open(success_file, "r") as _file:
        stac_items = json.loads(_file.read())

    for item in stac_items:
        submission_handler(
            event=item,
            cognito_app_secret=Variable.get("COGNITO_APP_SECRET"),
            stac_ingestor_api_url=Variable.get("STAC_INGESTOR_API_URL"),
        )
    return event


def cogify_task(ti):
    payload = ti.dag_run.conf
    return cogify_handler(payload)


def cogify_choice(ti, **kwargs):
    # Only get the payload from the successful task
    payload = ti.dag_run.conf
    if payload.get("cogify"):
        return f"{group_kwgs['group_id']}.cogify"

    return f"{group_kwgs['group_id']}.build_stac"


def subdag_process():
    with TaskGroup(**group_kwgs) as process_grp:
        cogify_branching = BranchPythonOperator(
            task_id="cogify_branching",
            trigger_rule="one_success",
            python_callable=cogify_choice,
        )
        mwaa_stack_conf = Variable.get("MWAA_STACK_CONF", deserialize_json=True)
        build_stac = EcsRunTaskOperator(
            task_id="build_stac",
            trigger_rule="none_failed",
            cluster=f"{mwaa_stack_conf.get('PREFIX')}-cluster",
            task_definition=f"{mwaa_stack_conf.get('PREFIX')}-tasks",
            launch_type="FARGATE",
            do_xcom_push=True,
            execution_timeout=timedelta(minutes=60),
            overrides={
                "containerOverrides": [
                    {
                        "name": f"{mwaa_stack_conf.get('PREFIX')}-veda-stac-build",
                        "command": [
                            "/usr/local/bin/python",
                            "handler.py",
                            "--payload",
                            "{}".format("{{ task_instance.dag_run.conf }}"),
                        ],
                        "environment": [
                            {
                                "name": "EXTERNAL_ROLE_ARN",
                                "value": Variable.get("ASSUME_ROLE_READ_ARN", default_var=None),
                            },
                            {
                                "name": "BUCKET",
                                "value": "veda-data-pipelines-staging-lambda-ndjson-bucket",
                            },
                            {
                                "name": "EVENT_BUCKET",
                                "value": mwaa_stack_conf.get("EVENT_BUCKET"),
                            },
                        ],
                        "memory": 2048,
                        "cpu": 1024,
                    },
                ],
            },
            network_configuration={
                "awsvpcConfiguration": {
                    "securityGroups": mwaa_stack_conf.get("SECURITYGROUPS"),
                    "subnets": mwaa_stack_conf.get("SUBNETS"),
                },
            },
            awslogs_group=mwaa_stack_conf.get("LOG_GROUP_NAME"),
            awslogs_stream_prefix=f"ecs/{mwaa_stack_conf.get('PREFIX')}-veda-stac-build",  # prefix with container name
        )
        cogify = PythonOperator(task_id="cogify", python_callable=cogify_task)
        submit_to_stac_ingestor = PythonOperator(
            task_id="submit_to_stac_ingestor",
            python_callable=submit_to_stac_ingestor_task,
        )
        cogify_branching >> build_stac >> submit_to_stac_ingestor
        cogify_branching >> cogify >> build_stac >> submit_to_stac_ingestor
        return process_grp
