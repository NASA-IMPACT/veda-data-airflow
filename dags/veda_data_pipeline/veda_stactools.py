from typing import Any, Dict, List
import pendulum
import json

from airflow.decorators import task

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
from stactools.core import use_fsspec

from veda_data_pipeline.groups.processing_tasks import submit_to_stac_ingestor_task
from veda_data_pipeline.groups.collection_group import ingest_collection_task


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
    "stactools_package_name": "sentinel2",
    "collection_id": "my_veda_sentinel_collection",
    "collection_params": {
        "additional_param": "for_collection"
    },
    "item_params": {
        "additional_param": "for_items"
    },
    "granules": [
        "https://sentinel-cogs.s3.us-west-2.amazonaws.com/sentinel-s2-l2a-cogs/37/S/DA/2020/8/S2A_37SDA_20200829_0_L2A/B04.tif",
        "https://sentinel-cogs.s3.us-west-2.amazonaws.com/sentinel-s2-l2a-cogs/37/S/DA/2020/8/S2A_37SDA_20200829_0_L2A/B08.tif"
    ]
}

@task
def upsert_stactools_collection(ti=None):
    body = {
        **ti.dag_run.conf,
    }
    stactools_package_name = body.get("stactools_package_name")
    stactools = __import__(f"stactools.{stactools_package_name}")

    collection_params = body.get("collection_params")
    params_dict = json.loads(collection_params)
    collection = stactools.stac.create_collection(
                **params_dict
            )
    collection.id = body.get("collection_id") # override collection id in case it is not set/supported in collection_params
    coll_dict = collection.to_dict()
    return coll_dict

@task
def build_items_from_granules(ti=None) -> List[str]:
    body = {
        **ti.dag_run.conf,
    }
    use_fsspec()

    stactools_package_name = body.get("stactools_package_name")
    stactools = __import__(f"stactools.{stactools_package_name}")

    output = []
    granule_list = body['granules']
    item_params = body['item_params']
    param_dict = json.loads(item_params)
    for granule in granule_list:
        stac = stactools.stac.create_item(granule, **param_dict)
        stac.collection_id = body['collection_id']
        output.append(stac)
    return output

params_dag_run_conf = template_dag_run_conf
with DAG(
    "veda_stactools",
    params=params_dag_run_conf,
    **dag_args
) as dag:
    # ECS dependency variable
    start = DummyOperator(task_id="Start", dag=dag)
    end = DummyOperator(
        task_id="End", trigger_rule=TriggerRule.ONE_SUCCESS, dag=dag
    )
    # define DAG using taskflow notation

    stactools_collection = upsert_stactools_collection()
    ingest_collection = ingest_collection_task(stactools_collection)

    get_items_from_granules = build_items_from_granules()
    submit_stac = submit_to_stac_ingestor_task.expand(built_stac=get_items_from_granules)
    submit_stac.set_upstream(ingest_collection)

    get_items_from_granules.set_upstream(start)
    stactools_collection.set_upstream(start)
    submit_stac.set_downstream(end)

