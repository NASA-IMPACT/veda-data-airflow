from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.amazon.aws.operators.ecs import (
    EcsDeregisterTaskDefinitionOperator,
    EcsRegisterTaskDefinitionOperator,
    EcsRunTaskOperator,
)
from airflow.hooks.base import BaseHook
import json


def get_aws_keys_from_connection(connection_id="aws_default"):
    conn = BaseHook.get_connection(connection_id)
    return {
        "AWS_ACCESS_KEY_ID": conn.login,
        "AWS_SECRET_ACCESS_KEY": conn.password,
        "AWS_DEFAULT_REGION": json.loads(conn.extra).get("region_name", "us-west-2"),
    }


group_kwgs = {"group_id": "ECSTasks", "tooltip": "ECSTasks"}


def subdag_ecs_task(
    task_id,
    task_definition_family,
    container_name,
    docker_image,
    cmd: str,
    mwaa_stack_conf,
    aws_region="us-west-2",
    cpu="256",
    memory="512",
    stage="dev",
    environment_vars=None,
):
    if environment_vars is None:
        environment_vars = list()
    with TaskGroup(**group_kwgs) as ecs_task_grp:
        if stage == "dev":
            from airflow.providers.docker.operators.docker import DockerOperator

            return DockerOperator(
                task_id=task_id,
                container_name=container_name,
                image=docker_image,
                api_version="auto",
                auto_remove=True,
                command=cmd,
                environment=get_aws_keys_from_connection(),
                docker_url="tcp://docker-in-docker:2375",
                mount_tmp_dir=False,
                network_mode="bridge",
            )

        register_task = EcsRegisterTaskDefinitionOperator(
            task_id=f"{task_id}_task_register",
            family=task_definition_family,
            trigger_rule=TriggerRule.ONE_SUCCESS,
            container_definitions=[
                {
                    "name": container_name,
                    "image": docker_image,
                    "entryPoint": ["sh", "-c"],
                    "command": ["ls"],
                    "logConfiguration": {
                        "logDriver": "awslogs",
                        "options": {
                            "awslogs-group": mwaa_stack_conf.get("LOG_GROUP_NAME"),
                            "awslogs-region": aws_region,
                            "awslogs-stream-prefix": "ecs",
                        },
                    },
                }
            ],
            register_task_kwargs={
                "cpu": cpu,
                "memory": memory,
                "networkMode": "awsvpc",
                "taskRoleArn": mwaa_stack_conf.get("MWAA_EXECUTION_ROLE_ARN"),
                "executionRoleArn": mwaa_stack_conf.get("MWAA_EXECUTION_ROLE_ARN"),
                "requiresCompatibilities": ["FARGATE"],
            },
        )
        ecs_task_run = EcsRunTaskOperator(
            task_id=task_id,
            cluster=mwaa_stack_conf.get("ECS_CLUSTER_NAME"),
            task_definition=register_task.output,
            launch_type="FARGATE",
            do_xcom_push=True,
            overrides={
                "containerOverrides": [
                    {
                        "name": container_name,
                        "command": [cmd],
                        "environment": environment_vars,
                    },
                ],
            },
            network_configuration={
                "awsvpcConfiguration": {
                    "securityGroups": mwaa_stack_conf.get("SECURITYGROUPS"),
                    "subnets": mwaa_stack_conf.get("SUBNETS"),
                },
            },
            awslogs_region="us-west-2",
            awslogs_group=mwaa_stack_conf.get("LOG_GROUP_NAME"),
            awslogs_stream_prefix=f"ecs/{container_name}",
        )
        deregister_task = EcsDeregisterTaskDefinitionOperator(
            task_id=f"{task_id}_deregister_task",
            task_definition=register_task.output,
        )

        register_task >> ecs_task_run >> deregister_task
        return ecs_task_grp
