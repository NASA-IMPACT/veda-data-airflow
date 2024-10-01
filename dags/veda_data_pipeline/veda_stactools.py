from typing import Any, Dict, List
import pendulum
import json

from airflow.decorators import task

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.models.variable import Variable
from stactools.core import use_fsspec
from stactools.noaa_hrrr.metadata import parse_href
from stactools.noaa_hrrr.stac import create_item

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
    "granules": "[List of granules]"
}

@task
def build_items_from_granules(ti=None) -> List[str]:
    body = {
        **ti.dag_run.conf,
    }
    use_fsspec()
    print(f'body: {body}')
    output = []
    href_list = body['granules']
    for href in href_list:
        print(f'href: {href}')
        href_parsed = parse_href(href)
        print(f'href_parsed: {href_parsed}')
        stac = create_item(**parse_href(href))
        stac.collection_id = "nrrr-stactools-test" # TODO change
        stac_dict = stac.to_dict()
        stac_dict['dry_run'] = True # TODO change to remove dry run
        output.append(json.dumps(stac_dict))
    print(f'output: {output}')
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

        get_items_from_granules = build_items_from_granules()
        submit_stac = submit_to_stac_ingestor_task.expand(built_stac=get_items_from_granules)

        get_items_from_granules.set_upstream(start)
        submit_stac.set_downstream(end)

        return dag

get_stactools_dag("veda_stactools")
