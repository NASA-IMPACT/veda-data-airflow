from typing import Any, Dict, List
import pendulum
import json

from datetime import timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.models.variable import Variable
from stactools.core import use_fsspec
from stactools.sentinel1.grd import Format
from stactools.sentinel1.grd.stac import create_item

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

@task()
def build_items_from_granules(message_list: List[Dict[Any,Any]]):
    use_fsspec()
    output = []
    for sentinel_message in message_list:
        href = f"s3://sentinel-s1-l1c/{sentinel_message['path']}"
        print(href)
        stac = create_item(
            granule_href=href, archive_format=Format.COG, requester_pays=True
        )
        stac.collection_id = "sentinel1-grd-stactools-test" # TODO change
        stac.dry_run = True
        output.append(json.dumps(stac.to_dict()))
    return output


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

        get_items_from_granules = build_items_from_granules # TODO placeholder
        submit_stac = submit_to_stac_ingestor_task.expand(built_stac=get_items_from_granules) # TODO change input

        get_items_from_granules.set_upstream(start) # TODO change parent
        submit_stac.set_downstream(end)

        return dag

get_stactools_dag("veda_stactools")
