import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.operators.dummy_operator import DummyOperator as EmptyOperator
from airflow.models.variable import Variable
import json
from veda_data_pipeline.groups.collection_group import collection_task_group
from veda_data_pipeline.groups.discover_group import discover_from_s3_task, get_dataset_files_to_process
from veda_data_pipeline.groups.processing_tasks import submit_to_stac_ingestor_task

dag_doc_md = """
### Dataset Pipeline
Generates a collection and triggers the file discovery process
#### Notes
- This DAG can run with the following configuration <br>
```json
{
    "collection": "collection-id", 
    "data_type": "cog", 
    "description": "collection description", 
    "discovery_items": 
        [
            {
                "bucket": "veda-data-store-staging", 
                "datetime_range": "year", 
                "discovery": "s3", 
                "filename_regex": "^(.*).tif$", 
                "prefix": "example-prefix/"
            }
        ], 
    "is_periodic": true, 
    "license": "collection-LICENSE", 
    "time_density": "year", 
    "title": "collection-title"
}
```
"""

dag_args = {
    "start_date": pendulum.today("UTC").add(days=-1),
    "schedule_interval": None,
    "catchup": False,
    "doc_md": dag_doc_md,
    "tags": ["collection", "discovery"],
}


@task
def extract_discovery_items(**kwargs):
    ti = kwargs.get("ti")
    discovery_items = ti.dag_run.conf.get("discovery_items")
    print(discovery_items)
    return discovery_items


@task(max_active_tis_per_dag=3)
def build_stac_task(payload):
    from veda_data_pipeline.utils.build_stac.handler import stac_handler
    airflow_vars = Variable.get("aws_dags_variables")
    airflow_vars_json = json.loads(airflow_vars)
    event_bucket = airflow_vars_json.get("EVENT_BUCKET")
    return stac_handler(payload_src=payload, bucket_output=event_bucket)


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
    "is_periodic": "<true|false>",
    "license": "<collection-LICENSE>",
    "time_density": "<time-density>",
    "title": "<collection-title>"
}

with DAG("veda_dataset_pipeline", params=template_dag_run_conf, **dag_args) as dag:
    # ECS dependency variable

    start = EmptyOperator(task_id="start", dag=dag)
    end = EmptyOperator(task_id="end", dag=dag)

    collection_grp = collection_task_group()
    discover = discover_from_s3_task.expand(event=extract_discovery_items())
    discover.set_upstream(collection_grp)  # do not discover until collection exists
    get_files = get_dataset_files_to_process(payload=discover)

    build_stac = build_stac_task.expand(payload=get_files)
    # .output is needed coming from a non-taskflow operator
    submit_stac = submit_to_stac_ingestor_task.expand(built_stac=build_stac)

    collection_grp.set_upstream(start)
    submit_stac.set_downstream(end)
