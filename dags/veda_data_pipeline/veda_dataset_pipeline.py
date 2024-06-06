import pendulum
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator as EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from veda_data_pipeline.groups.collection_group import collection_task_group
from veda_data_pipeline.groups.discover_group import subdag_discover

dag_doc_md = """
### Dataset Pipeline
Generates a collection and triggers the file discovery process 
"""

dag_args = {
    "start_date": pendulum.today("UTC").add(days=-1),
    "schedule_interval": None,
    "catchup": False,
    "doc_md": dag_doc_md,
    "tags": ["collection", "discovery"],
}

with DAG("veda_dataset_pipeline", **dag_args) as dag:
    start = EmptyOperator(task_id="start", dag=dag)
    end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ONE_SUCCESS, dag=dag)

    collection_grp = collection_task_group()
    discover_grp = subdag_discover()

    start >> collection_grp >> discover_grp >> end
