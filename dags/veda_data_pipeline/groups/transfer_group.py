from airflow.sdk import Variable
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from airflow.decorators import task

group_kwgs = {"group_id": "Transfer", "tooltip": "Transfer"}


def cogify_choice(dag_run=None):
    """Choose whether to cogify or not; if yes, use a docker container"""
    payload = dag_run.conf

    if payload.get("cogify") == "true" or payload.get("cogify") == True:
        return f"{group_kwgs['group_id']}.cogify_and_copy_data"
    else:
        return f"{group_kwgs['group_id']}.copy_data"


def cogify_copy_task(dag_run=None):
    from veda_data_pipeline.utils.cogify_transfer.handler import cogify_transfer_handler
    config = dag_run.conf
    external_role_arn = Variable.get("ASSUME_ROLE_WRITE_ARN")
    return cogify_transfer_handler(event_src=config, external_role_arn=external_role_arn)

@task
def transfer_data_task(dag_run=None, payload={}):
    """Transfer data from one S3 bucket to another; s3 copy, no need for docker"""
    # use task-provided payload if provided, otherwise fall back on dag_run conf values
    # payload will generally have the same values expected by discovery, so some renames are needed when combining the dicts
    config = {
        **payload,
        **dag_run.conf,
        "origin_bucket": payload.get("bucket", dag_run.conf.get("origin_bucket", "veda-data-store")),
        "origin_prefix": payload.get("prefix", dag_run.conf.get("origin_prefix", "s3-prefix/")),
        "target_bucket": payload.get("target_bucket", dag_run.conf.get("target_bucket", "veda-data-store")),
        "dry_run": payload.get("dry_run", dag_run.conf.get("dry_run", False)),
    }
    transfer_data(payload=config)

# non-decorated function for use in other tasks
def transfer_data(dag_run=None, payload={}):
    """Transfer data from one S3 bucket to another; s3 copy, no need for docker"""
    from veda_data_pipeline.utils.transfer import (
        data_transfer_handler,
    )
    external_role_arn = Variable.get("ASSUME_ROLE_WRITE_ARN")
    # (event, chunk_size=2800, role_arn=None, bucket_output=None):
    if payload == {}:
        payload = dag_run.conf
    return data_transfer_handler(event=payload, role_arn=external_role_arn)

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
