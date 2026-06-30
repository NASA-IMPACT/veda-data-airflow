import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.models.param import Param
from veda_data_pipeline.groups.discover_group import discover_from_s3_task, get_files_task
from slack_notifications import slack_fail_alert

from veda_data_pipeline.groups.processing_tasks import submit_to_stac_ingestor_task, build_stac_task

dag_doc_md = """
### Discover files from S3
#### Purpose
This DAG discovers files from either S3 and/or CMR then runs a DAG id `veda_ingest`.
The DAG `veda_ingest` will run in parallel processing (2800 files per each DAG)
#### Notes
- This DAG can run with the following configuration <br>
```json
{
    "collection": "collection-id",
    "bucket": "veda-data-store-staging",
    "prefix": "s3-prefix/",
    "filename_regex": "^(.*).tif$",
    "id_regex": ".*_(.*).tif$",
    "process_from_yyyy_mm_dd": "YYYY-MM-DD",
    "id_template": "example-id-prefix-{}",
    "datetime_range": "month",
    "last_successful_execution": datetime(2015,01,01),
    "assets": {
        "asset1": {
            "title": "Asset type 1",
            "description": "First of a multi-asset item.",
            "regex": ".*asset1.*",
        },
        "asset2": {
            "title": "Asset type 2",
            "description": "Second of a multi-asset item.",
            "regex": ".*asset2.*",
        },
    },
    "request_payer": False
}
```
- [Supports linking to external content](https://github.com/NASA-IMPACT/veda-data-pipelines)
"""

dag_args = {
    "start_date": pendulum.today("UTC").add(days=-1),
    "catchup": False,
    "doc_md": dag_doc_md,
    "on_failure_callback": slack_fail_alert,
    "is_paused_upon_creation": False,
}

template_dag_run_conf = {
    "collection": Param("collection_name", type="string"),
    "bucket": "<bucket>",
    "prefix": "<prefix>/",
    "filename_regex": "<file_regex>",
    "id_regex": "<id_regex>",
    "id_template": "<id_template_string>",
    "datetime_range": Param(type="string", enum=["year","month", "day", ""], description="<year|month|day>", default=""),
    "assets": {
        "<asset1_name>": {
            "title": "<asset_title>",
            "description": "<asset_description>",
            "regex": "<asset_regex>",
        },
        "<asset2_name>": {
            "title": "<asset_title>",
            "description": "<asset_description>",
            "regex": "<asset_regex>",
        },
    },
    "request_payer": Param(
            type=bool,
            description="Use 'requester' to confirm charge for the request on bucket with Requester Pays enabled",
            default=False
        )
}

def get_discover_dag(id: str, event: dict):

    with DAG(
            id,
            schedule=event.get("schedule"),
            params=template_dag_run_conf,
            **dag_args
    ) as dag:
        start = EmptyOperator(task_id="Start", dag=dag)
        end = EmptyOperator(
            task_id="End", dag=dag
        )
        # define DAG using taskflow notation

        discover = discover_from_s3_task(event=event)
        get_files = get_files_task(payload=discover)
        build_stac = build_stac_task.expand(payload=get_files)
        # .output is needed coming from a non-taskflow operator
        submit_stac = submit_to_stac_ingestor_task.expand(built_stac=build_stac)

        start >> discover
        submit_stac >> end
        
        return dag

# Sending empty event because we rely on task instance (ti) for manual runs
# and payload for scheduled runs
# get_discover_dag(id="veda_discover", event={})
