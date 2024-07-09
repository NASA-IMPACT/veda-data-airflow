import pendulum
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator as EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from veda_data_pipeline.groups.collection_group import collection_task_group
from veda_data_pipeline.groups.discover_group import subdag_discover

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
    start = EmptyOperator(task_id="start", dag=dag)
    end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ONE_SUCCESS, dag=dag)

    collection_grp = collection_task_group()
    discover_grp = subdag_discover()

    start >> collection_grp >> discover_grp >> end
