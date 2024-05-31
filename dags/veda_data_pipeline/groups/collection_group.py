from typing import Union
from fastapi import Body

from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from src.schemas import (
    DashboardCollection,
    ZarrDataset,
    COGDataset,
)
from utils.collection_publisher import Publisher

from veda_data_pipeline.utils.submit_stac import (
    submission_handler
)

publisher = Publisher()

def ingest_collection(
        dataset: Union[ZarrDataset, COGDataset]
    ):
    """
    Ingests a dataset collection

    Args:
        dataset (Union[ZarrDataset, COGDataset]): The dataset to ingest
    """
    collection_data = publisher.generate_stac(dataset, dataset.data_type or "cog")
    collection = DashboardCollection.parse_obj(collection_data)

    return submission_handler(
        event=collection
    )

group_kwgs = {"group_id": "Collection", "tooltip": "Collection"}

def generate_collection_task(ti):
    config = ti.dag_run.conf

    return ingest_collection(dataset=config)

def generate_collection():
    with TaskGroup(**group_kwgs) as collection_grp:
        run_generate_collection = PythonOperator(
            task_id="generate_collection",
            python_callable=generate_collection_task,
            trigger_rule=TriggerRule.ONE_SUCCESS
        )

        run_generate_collection

        return collection_grp
