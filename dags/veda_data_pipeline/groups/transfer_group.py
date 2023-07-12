from datetime import timedelta

from airflow.models.variable import Variable
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.amazon.aws.operators.ecs import EcsRunTaskOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from veda_data_pipeline.utils.transfer import (
    data_transfer_handler,
)

group_kwgs = {"group_id": "Transfer", "tooltip": "Transfer"}


def cogify_choice(ti):
    """Choos whether to cogify or not; if yes, use a docker container"""
    payload = ti.dag_run.conf

    if payload.get("cogify"):
        return f"{group_kwgs['group_id']}.cogify_and_copy_data"
    else:
        return f"{group_kwgs['group_id']}.copy_data"


def transfer_data(ti):
    """Transfer data from one S3 bucket to another; s3 copy, no need for docker"""
    config = ti.dag_run.conf
    role_arn = Variable.get("ASSUME_ROLE_READ_ARN")
    # (event, chunk_size=2800, role_arn=None, bucket_output=None):
    return data_transfer_handler(event=config, role_arn=role_arn)


def subdag_transfer():
    with TaskGroup(**group_kwgs) as discover_grp:
        cogify_branching = BranchPythonOperator(
            task_id="cogify_branching",
            trigger_rule=TriggerRule.ONE_SUCCESS,
            python_callable=cogify_choice,
        )

        run_copy = PythonOperator(
            task_id="copy_data",
            python_callable=transfer_data,
            op_kwargs={"text": "Copy files on S3"},
        )

        mwaa_stack_conf = Variable.get("MWAA_STACK_CONF", deserialize_json=True)
        run_cogify_copy = EcsRunTaskOperator(
            task_id="cogify_and_copy_data",
            trigger_rule="none_failed",
            cluster=f"{mwaa_stack_conf.get('PREFIX')}-cluster",
            task_definition=f"{mwaa_stack_conf.get('PREFIX')}-tasks",
            launch_type="FARGATE",
            do_xcom_push=True,
            execution_timeout=timedelta(minutes=120),
            overrides={
                "containerOverrides": [
                    {
                        "name": f"{mwaa_stack_conf.get('PREFIX')}-veda-cogify-transfer",
                        "command": [
                            "/usr/local/bin/python",
                            "handler.py",
                            "--payload",
                            "{}".format("{{ task_instance.dag_run.conf }}"),
                        ],
                        "environment": [
                            {
                                "name": "EXTERNAL_ROLE_ARN",
                                "value": Variable.get("ASSUME_ROLE_READ_ARN"),
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
            awslogs_stream_prefix=f"ecs/{mwaa_stack_conf.get('PREFIX')}-veda-cogify-transfer",  # prefix with container name
        )

        (cogify_branching >> [run_copy, run_cogify_copy])
        return discover_grp
