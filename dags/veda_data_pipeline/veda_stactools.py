import pendulum

from datetime import timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.models.variable import Variable

from veda_data_pipeline.groups.processing_tasks import submit_to_stac_ingestor_task


dag_doc_md = """
Stactools ingestion POC
"""

dag_args = {
    "start_date": pendulum.today("UTC").add(days=-1),
    "catchup": False,
    "doc_md": dag_doc_md,
    "is_paused_upon_creation": False,
}

template_dag_run_conf = {
    "collection": "<coll_name>"
}


def get_stactools_dag(id, event={}):
    params_dag_run_conf = event or template_dag_run_conf
    with DAG(
        id,
        schedule_interval=event.get("schedule"),
        params=params_dag_run_conf,
        **dag_args
    ) as dag:
        # ECS dependency variable
        mwaa_stack_conf = Variable.get("MWAA_STACK_CONF", deserialize_json=True)

        start = DummyOperator(task_id="Start", dag=dag)
        end = DummyOperator(
            task_id="End", trigger_rule=TriggerRule.ONE_SUCCESS, dag=dag
        )
        # define DAG using taskflow notation
        
        submit_stac = submit_to_stac_ingestor_task.expand(built_stac={}) # TODO change input

        discover = None # TODO placeholder
        discover.set_upstream(start) # TODO change parent
        submit_stac.set_downstream(end)

        return dag

get_stactools_dag("veda_stactools")
