import pendulum
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator as EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from veda_data_pipeline.groups.collection_group import collection_task_group

dag_doc_md = """
### Collection Creation and Ingestion
Generates a collection based on the Dataset model and ingests into the catalog 
#### Notes
- This DAG can run with the following configuration <br>
```json
{
    "collection": "collection-id", 
    "data_type": "cog", 
    "description": "collection description",
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
    "tags": ["collection"],
}

template_dag_run_conf = {
    "collection": "<collection-id>", 
    "data_type": "cog", 
    "description": "<collection-description>", 
    "is_periodic": "<true|false>", 
    "license": "<collection-LICENSE>", 
    "time_density": "<time-density>", 
    "title": "<collection-title>"
}

with DAG("veda_collection_pipeline", params=template_dag_run_conf, **dag_args) as dag:
    start = EmptyOperator(task_id="start", dag=dag)
    end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS, dag=dag)

    collection_grp = collection_task_group()

    start >> collection_grp >> end
