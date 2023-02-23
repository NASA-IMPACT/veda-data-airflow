import json
import logging
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.models.variable import Variable
import smart_open

from veda_data_pipeline.groups.ecs_tasks import subdag_ecs_task
from veda_data_pipeline.veda_pipeline_tasks.submit_stac.handler import (
    submission_handler,
)
from veda_data_pipeline.veda_pipeline_tasks.cogify.handler import cogify_handler


group_kwgs = {"group_id": "Process", "tooltip": "Process"}
subgroup_kwgs = {"ecs_group_id": "ECSTasks", "subdag_ecs_task_id": "build_stac"}


def log_task(text: str):
    logging.info(text)


def submit_to_stac_ingestor_task(ti):
    print("Submit STAC ingestor")
    event = json.loads(
        ti.xcom_pull(
            task_ids=f"{group_kwgs['group_id']}.{subgroup_kwgs['ecs_group_id']}.{subgroup_kwgs['subdag_ecs_task_id']}"
        )
    )

    success_file = event["payload"]["success_event_key"]
    with smart_open.open(success_file, "r") as _file:
        stac_items = json.loads(_file.read())
    cognito_app_secret = Variable.get("COGNITO_APP_SECRET")
    stac_ingestor_api_url = Variable.get("STAC_INGESTOR_API_URL")
    for item in stac_items:
        submission_handler(
            event={ "stac_item": item },
            cognito_app_secret=cognito_app_secret,
            stac_ingestor_api_url=stac_ingestor_api_url,
        )
    return event


def cogify_task(ti):
    payload = ti.dag_run.conf
    earthdata_username = None
    earthdata_password = None
    return cogify_handler(
        file_uri=payload,
        earthdata_username=earthdata_username,
        earthdata_password=earthdata_password,
    )


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
            task_id=subgroup_kwgs["subdag_ecs_task_id"],
            task_definition_family="ecs_veda_family",
            cpu="1024",
            memory="2048",
            cmd='/usr/local/bin/python handler.py --payload "{}"'.format(
                "{{ task_instance.dag_run.conf }}"
            ),
            container_name=f"{prefix}-veda-build_stac",
            docker_image=f"{acct_id}.dkr.ecr.{region}.amazonaws.com/{prefix}-veda-build_stac",
            mwaa_stack_conf=MWAA_STACK_CONF.copy(),
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
