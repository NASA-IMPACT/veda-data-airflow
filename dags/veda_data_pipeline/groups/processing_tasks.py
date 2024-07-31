import json
import logging

import smart_open
from airflow.models.variable import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.ecs import EcsRunTaskOperator
from airflow.decorators import task_group, task
from veda_data_pipeline.utils.submit_stac import submission_handler

group_kwgs = {"group_id": "Process", "tooltip": "Process"}


def log_task(text: str):
    logging.info(text)

@task()
def submit_to_stac_ingestor_task(built_stac:str):
    """Submit STAC items to the STAC ingestor API."""
    event = json.loads(built_stac)
    success_file = event["payload"]["success_event_key"]
    with smart_open.open(success_file, "r") as _file:
        stac_items = json.loads(_file.read())

    for item in stac_items:
        submission_handler(
            event=item,
            endpoint="/ingestions",
            cognito_app_secret=Variable.get("COGNITO_APP_SECRET"),
            stac_ingestor_api_url=Variable.get("STAC_INGESTOR_API_URL"),
        )
    return event

@task
def build_stac_kwargs(event={}):
    """Build kwargs for the ECS operator."""
    mwaa_stack_conf = Variable.get("MWAA_STACK_CONF", deserialize_json=True)
    if event:
        intermediate = {
            **event
        } # this is dumb but it resolves the MappedArgument to a dict that can be JSON serialized
        payload = json.dumps(intermediate)
    else:
        payload = "{{ task_instance.dag_run.conf }}"

    return {
        "overrides": {
            "containerOverrides": [
                {
                    "name": f"{mwaa_stack_conf.get('PREFIX')}-veda-stac-build",
                    "command": [
                        "/usr/local/bin/python",
                        "handler.py",
                        "--payload",
                        payload,
                    ],
                    "environment": [
                        {
                            "name": "EXTERNAL_ROLE_ARN",
                            "value": Variable.get(
                                "ASSUME_ROLE_READ_ARN", default_var=""
                            ),
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
        "network_configuration": {
            "awsvpcConfiguration": {
                "securityGroups": mwaa_stack_conf.get("SECURITYGROUPS"),
                "subnets": mwaa_stack_conf.get("SUBNETS"),
            },
        },
        "awslogs_group": mwaa_stack_conf.get("LOG_GROUP_NAME"),
        "awslogs_stream_prefix": f"ecs/{mwaa_stack_conf.get('PREFIX')}-veda-stac-build",
    }

@task
def build_vector_kwargs(event={}):
    """Build kwargs for the ECS operator."""
    mwaa_stack_conf = Variable.get(
        "MWAA_STACK_CONF", default_var={}, deserialize_json=True
    )
    vector_ecs_conf = Variable.get(
        "VECTOR_ECS_CONF", default_var={}, deserialize_json=True
    )

    if event:
        intermediate = {
            **event
        }
        payload = json.dumps(intermediate)
    else:
        payload = "{{ task_instance.dag_run.conf }}"
    
    return {
        "trigger_rule": "none_failed",
        "cluster": f"{mwaa_stack_conf.get('PREFIX')}-cluster",
        "task_definition": f"{mwaa_stack_conf.get('PREFIX')}-vector-tasks",
        "launch_type": "FARGATE",
        "do_xcom_push": True,
        "execution_timeout": timedelta(minutes=120),
        "overrides": {
            "containerOverrides": [
                {
                    "name": f"{mwaa_stack_conf.get('PREFIX')}-veda-vector_ingest",
                    "command": [
                        "/var/lang/bin/python",
                        "handler.py",
                        "--payload",
                        payload,
                    ],
                    "environment": [
                        {
                            "name": "EXTERNAL_ROLE_ARN",
                            "value": Variable.get(
                                "ASSUME_ROLE_READ_ARN", default_var=None
                            ),
                        },
                        {
                            "name": "AWS_REGION",
                            "value": mwaa_stack_conf.get("AWS_REGION"),
                        },
                        {
                            "name": "VECTOR_SECRET_NAME",
                            "value": Variable.get("VECTOR_SECRET_NAME"),
                        },
                    ],
                },
            ],
        },
        "network_configuration": {
            "awsvpcConfiguration": {
                "securityGroups": vector_ecs_conf.get("VECTOR_SECURITY_GROUP"),
                "subnets": vector_ecs_conf.get("VECTOR_SUBNETS"),
            },
        },
        "awslogs_group": mwaa_stack_conf.get("LOG_GROUP_NAME"),
        "awslogs_stream_prefix": f"ecs/{mwaa_stack_conf.get('PREFIX')}-veda-vector_ingest",
    }


@task_group
def subdag_process(event={}):

    build_stac = EcsRunTaskOperator.partial(
        task_id="build_stac"
    ).expand_kwargs(build_stac_kwargs(event=event))

    submit_to_stac_ingestor = PythonOperator(
        task_id="submit_to_stac_ingestor",
        python_callable=submit_to_stac_ingestor_task,
    )

    build_stac >> submit_to_stac_ingestor