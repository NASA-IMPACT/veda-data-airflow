from typing import List
import pendulum
from importlib import import_module

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
    "stactools_package_name": "cop_dem",
    "collection_id": "my_veda_cop_dem_collection",
    "collection_params": {
        "product": "glo-30"
    },
    "item_params": {
        "host": "AWS"
    },
    "granules": [
        "s3://copernicus-dem-30m/Copernicus_DSM_COG_10_S84_00_E062_00_DEM/Copernicus_DSM_COG_10_S84_00_E062_00_DEM.tif"
    ]
}

@task
def upsert_stactools_collection(ti=None):
    body = {
        **ti.dag_run.conf,
    }
    stactools_package_name = body.get("stactools_package_name")
    stactools = import_module(f".{stactools_package_name}.stac", "stactools")

    collection_params = body.get("collection_params")
    try:
        collection = stactools.create_collection(
                    **collection_params
                )
    except AttributeError:
        # TODO  we should support a default collection creation process here, following a similar pattern to stactools
        # ref - sentinel2 doesn't provide create_collection, but landsat and other packages do
        raise AttributeError(f"Collection creation is not supported for {stactools_package_name}")
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
    stactools = import_module(f".{stactools_package_name}.stac", "stactools")

    output = []
    granule_list = body.get('granules')
    item_params = body.get("item_params")
    for granule in granule_list:
        try:
            stac = stactools.create_item(granule, **item_params)
        except AttributeError:
            # TODO we should support a default item creation process here, following a similar pattern to stactools
            # this is lower priority to support than collection creation - create_item() is more universal
            raise AttributeError(f"Item creation is not supported for {stactools_package_name}")
        stac.collection_id = body['collection_id']
        # convert to dict if not already (pystac `Item` is common)
        if not isinstance(stac, dict):
            stac = stac.to_dict()
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

