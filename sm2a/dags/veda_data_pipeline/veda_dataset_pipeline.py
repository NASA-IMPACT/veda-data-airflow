import pendulum
from airflow import DAG
from airflow.models.param import Param
from veda_data_pipeline.groups.discover_group import discover_from_s3_task, get_files_task
from airflow.operators.empty import EmptyOperator
from veda_data_pipeline.groups.collection_group import collection_task_group
from veda_data_pipeline.groups.processing_tasks import submit_to_stac_ingestor_task, build_stac_task, extract_discovery_items_from_payload, remove_thumbnail_asset

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
    "schedule": None,
    "catchup": False,
    "doc_md": dag_doc_md,
    "tags": ["collection", "discovery"],
}

with DAG("veda_dataset_pipeline", params=template_dag_run_conf, **dag_args) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    mutated_payloads = start >> collection_task_group() >> remove_thumbnail_asset()
    discovery_items = extract_discovery_items_from_payload(payload=mutated_payloads)
    discover = discover_from_s3_task.partial(payload=mutated_payloads).expand(event=discovery_items)
    get_files = get_files_task(payload=discover)
    build_stac = build_stac_task.expand(payload=get_files)
    submit_stac = submit_to_stac_ingestor_task.expand(built_stac=build_stac) >> end
