import pendulum
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator as EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from veda_data_pipeline.groups.discover_group import subdag_discover
from veda_data_pipeline.groups.collection_group import generate_collection

dag_doc_md = """
### Dataset Pipeline
Generates a collection 
"""

dag_args = {
    "start_date": pendulum.today("UTC").add(days=-1),
    "schedule_interval": None,
    "catchup": False,
    "doc_md": dag_doc_md,
    "tags": ["dataset", "collection"],
}

with DAG("veda_dataset_pipeline", **dag_args) as dag:
    start = EmptyOperator(task_id="start", dag=dag)
    end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ONE_SUCCESS, dag=dag)

    collection_grp = generate_collection()
    discover_grp = subdag_discover()

    start >> collection_grp >> discover_grp >> end
