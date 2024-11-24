import pendulum
from airflow import DAG
from airflow.models.param import Param
from airflow.operators.dummy_operator import DummyOperator as EmptyOperator
from airflow_multi_dagrun.operators import TriggerMultiDagRunOperator
from veda_data_pipeline.groups.collection_group import collection_task_group

template_dag_run_conf = {
    "collection": "<collection-id>",
    "data_type": "cog",
    "description": "<collection-description>",
    "discovery_items": [
        {
            "bucket": "<bucket-name>",
            "datetime_range": "<range>",
            "discovery": "s3",
            "filename_regex": "<regex>",
            "prefix": "<example-prefix/>",
        }
    ],
    "is_periodic": "<true|false>",
    "license": "<collection-LICENSE>",
    "time_density": "<time-density>",
    "title": "<collection-title>",
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


def trigger_discover_and_build_task(ti):
    discovery_items = ti.dag_run.conf.get("discovery_items")
    for discovery_item in discovery_items:
        yield discovery_item


with DAG("veda_dataset_pipeline", params=template_dag_run_conf, **dag_args) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    run_discover_build_and_push = TriggerMultiDagRunOperator(
        task_id="trigger_discover_items_dag",
        dag=dag,
        trigger_dag_id="veda_discover",
        python_callable=trigger_discover_and_build_task,
    )

    start >> collection_task_group() >> run_discover_build_and_push >> end
