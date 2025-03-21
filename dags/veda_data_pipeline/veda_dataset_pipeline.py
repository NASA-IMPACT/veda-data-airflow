import pendulum
from airflow import DAG
from airflow.models.param import Param
from veda_data_pipeline.groups.discover_group import discover_from_s3_task, get_files_task
from airflow.operators.dummy_operator import DummyOperator as EmptyOperator
from veda_data_pipeline.groups.collection_group import collection_task_group
from veda_data_pipeline.groups.processing_tasks import submit_to_stac_ingestor_task, build_stac_task, extract_discovery_items_from_payload, remove_thumbnail_asset
from veda_data_pipeline.groups.failure_management_group import notify_failure, review_build_stac_failures
import smart_open
import json

from airflow.decorators import task

template_dag_run_conf = {
    "collection": "<collection-id>",
    "data_type": "cog",
    "description": "<collection-description>",
    "discovery_items":
        [
            {
                "bucket": "<bucket-name>",
                "datetime_range": "<range>",
                "discovery": "s3",
                "filename_regex": "<regex>",
                "prefix": "<example-prefix/>"
            }
        ],
    "is_periodic": Param(True, type="boolean"),
    "license": "<collection-LICENSE>",
    "time_density": "<time-density>",
    "title": "<collection-title>"
}

dag_doc_md = f"""
### Dataset Pipeline
Generates a collection and triggers the file discovery process
#### Notes
- This DAG can run with the following configuration <br>
```json
{template_dag_run_conf}
```
"""

dag_args = {
    "start_date": pendulum.today("UTC").add(days=-1),
    "schedule_interval": None,
    "catchup": False,
    "doc_md": dag_doc_md,
    "tags": ["collection", "discovery"],
}

@task.branch
def branch_on_build_success_task(payload) -> str:
    if payload['payload']['status']['failures'] > 0:
        return "review_build_stac_failures"
    else:
        return "submit_to_stac_ingestor_task"

with DAG("veda_dataset_pipeline", params=template_dag_run_conf, **dag_args) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    mutated_payloads = start >> collection_task_group() >> remove_thumbnail_asset()

    discovery_items = extract_discovery_items_from_payload(payload=mutated_payloads)
    discover = discover_from_s3_task.partial(payload=mutated_payloads).expand(event=discovery_items)
    get_files = get_files_task(payload=discover)
    build_stac = build_stac_task.expand(payload=get_files)
    branch_on_build_success = branch_on_build_success_task.expand(payload=build_stac)
    submit_stac = submit_to_stac_ingestor_task.expand(built_stac=build_stac) # TODO review trigger
    review = review_build_stac_failures.expand(built_stac_report=build_stac)
    filtered_stac = review["filtered_stac"]
    submit_filtered_stac = submit_to_stac_ingestor_task(built_stac=filtered_stac)
    notify = notify_failure(status="failed", emoji="red-circle", failed_items=review["failed_items"])

    branch_on_build_success >> [submit_stac, review]
    review >> notify >> end
    review >> submit_filtered_stac >> end
    submit_stac >> end

