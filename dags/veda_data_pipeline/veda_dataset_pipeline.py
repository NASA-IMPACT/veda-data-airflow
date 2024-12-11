import pendulum
from airflow import DAG
from copy import deepcopy
from airflow.models.param import Param
from airflow.decorators import task
from veda_data_pipeline.groups.discover_group import discover_from_s3_task, get_files_task
from airflow.operators.dummy_operator import DummyOperator as EmptyOperator
from veda_data_pipeline.groups.collection_group import collection_task_group
from veda_data_pipeline.groups.processing_tasks import submit_to_stac_ingestor_task, build_stac_task

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

with DAG("veda_dataset_pipeline", params=template_dag_run_conf, **dag_args) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")


    @task()
    def remove_thumbnail_asset(ti):
        payload = deepcopy(ti.dag_run.conf)
        payloads = list()
        assets = payload.get("assets", {})
        if assets.get("thumbnail"):
            assets.pop("thumbnail")
        # if thumbnail was only asset, delete assets
        if not assets:
            payload.pop("assets")
        for item in payload.get("discovery_items"):
            payloads.append({
                **payload,
                **item
            }
            )

        return payloads


    mutated_payloads = start >> collection_task_group() >> remove_thumbnail_asset()
    discover = discover_from_s3_task.expand(payload=mutated_payloads)
    get_files = get_files_task(payload=discover)
    build_stac = build_stac_task.expand(payload=get_files)
    submit_stac = submit_to_stac_ingestor_task.expand(built_stac=build_stac) >> end
