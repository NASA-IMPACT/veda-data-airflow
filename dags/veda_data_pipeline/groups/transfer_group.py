from datetime import timedelta

from airflow.models.variable import Variable
from airflow.operators.python import BranchPythonOperator, PythonOperator
import json
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

group_kwgs = {"group_id": "Transfer", "tooltip": "Transfer"}


def cogify_choice(ti):
    """Choos whether to cogify or not; if yes, use a docker container"""
    payload = ti.dag_run.conf

    if payload.get("cogify"):
        return f"{group_kwgs['group_id']}.cogify_and_copy_data"
    else:
        return f"{group_kwgs['group_id']}.copy_data"


def cogify_copy_task(ti):
    from veda_data_pipeline.utils.cogify_transfer.handler import cogify_transfer_handler
    config = ti.dag_run.conf
    airflow_vars = Variable.get("aws_dags_variables")
    airflow_vars_json = json.loads(airflow_vars)
    external_role_arn = airflow_vars_json.get("ASSUME_ROLE_WRITE_ARN")
    return cogify_transfer_handler(event_src=config, external_role_arn=external_role_arn)


def transfer_data(ti):
    """Transfer data from one S3 bucket to another; s3 copy, no need for docker"""
    from veda_data_pipeline.utils.transfer import (
        data_transfer_handler,
    )
    config = ti.dag_run.conf
    airflow_vars = Variable.get("aws_dags_variables")
    airflow_vars_json = json.loads(airflow_vars)
    external_role_arn = airflow_vars_json.get("ASSUME_ROLE_WRITE_ARN")
    # (event, chunk_size=2800, role_arn=None, bucket_output=None):
    return data_transfer_handler(event=config, role_arn=external_role_arn)


# TODO: cogify_transfer handler is missing arg parser so this subdag will not work
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
        run_cogify_copy = PythonOperator(
            task_id="cogify_and_copy_data",
            trigger_rule="none_failed",
            python_callable=cogify_copy_task
        )

        (cogify_branching >> [run_copy, run_cogify_copy])
        return discover_grp
