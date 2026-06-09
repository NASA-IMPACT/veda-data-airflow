from typing import List
import pendulum
from slack_notifications import slack_fail_alert

from airflow.decorators import task

from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from stactools.core import use_fsspec
from stactools.noaa_hrrr.metadata import parse_href, CloudProvider, Product, Region
from stactools.noaa_hrrr.stac import create_item, create_collection

from veda_data_pipeline.groups.processing_tasks import submit_to_stac_ingestor_task_direct
from veda_data_pipeline.groups.collection_group import ingest_collection_task


dag_doc_md = """
Stactools ingestion POC
"""

dag_args = {
    "start_date": pendulum.today("UTC").add(days=-1),
    "catchup": False,
    "doc_md": dag_doc_md,
    "on_failure_callback": slack_fail_alert,
    "is_paused_upon_creation": False,
}

template_dag_run_conf = {
    "granules": ["https://noaahrrr.blob.core.windows.net/hrrr/hrrr.20240613/conus/hrrr.t11z.wrfsfcf06.grib2"],
    "collection_id": "nrrr-stactools-test",
    "region": "conus",
    "product": "sfc",
    "cloud_provider": "azure"
}


@task
def build_items_from_granules(ti=None) -> List[dict]:
    body = {
        **ti.dag_run.conf,
    }
    use_fsspec()
    print(f'body: {body}')
    output = []
    href_list = body['granules']
    for href in href_list:
        stac = create_item(**parse_href(href))
        stac.collection_id = body['collection_id']
        stac_dict = stac.to_dict()
        output.append(stac_dict)
    return output

@task
def upsert_stactools_collection(ti=None):
    body = {
        **ti.dag_run.conf,
    }
    region = body.get("region")
    product = body.get("product")
    cloud_provider = body.get("cloud_provider")

    collection = create_collection(
                region=Region(region), product=Product(product), cloud_provider=CloudProvider(cloud_provider)
            )
    collection.id = body.get("collection_id")
    coll_dict = collection.to_dict()
    return coll_dict

def get_stactools_dag(id, event={}):
    params_dag_run_conf = event or template_dag_run_conf
    with DAG(
        id,
        schedule=event.get("schedule", None),
        params=params_dag_run_conf,
        **dag_args
    ) as dag:
        # ECS dependency variable
        start = EmptyOperator(task_id="Start", dag=dag)
        end = EmptyOperator(
            task_id="End", trigger_rule=TriggerRule.ONE_SUCCESS, dag=dag
        )
        # define DAG using taskflow notation

        stactools_collection = upsert_stactools_collection()
        ingest_collection = ingest_collection_task(collection=stactools_collection)

        get_items_from_granules = build_items_from_granules()
        submit_stac = submit_to_stac_ingestor_task_direct.expand(stac_items=get_items_from_granules)
        ingest_collection >> submit_stac

        start >> [get_items_from_granules, stactools_collection]
        submit_stac >> end

        return dag

get_stactools_dag("veda_stactools_noaa_hrrr")
