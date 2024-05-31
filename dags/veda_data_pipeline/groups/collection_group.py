from typing import Dict, Any

from airflow.models.variable import Variable
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

from veda_data_pipeline.utils.collection_generation import GenerateCollection
from veda_data_pipeline.utils.submit_stac import (
    submission_handler
)

generator = GenerateCollection()

def ingest_collection(
        dataset: Dict[str, Any],
        data_type: str = "cog",
        role_arn: str = None
    ):
    """
    Ingest a collection into the STAC catalog

    Args:
        dataset (Dict[str, Any]): dataset dictionary (JSON)
        data_type (str): collection data type, defaults to "cog"
        role_arn (str): role arn for Zarr collection generation
    """
    collection = generator.generate_stac(dataset, data_type, role_arn)

    return submission_handler(
        event=collection,
        endpoint="/collections"
    )

group_kwgs = {"group_id": "Collection", "tooltip": "Collection"}

def generate_collection_task(ti):
    config = ti.dag_run.conf
    role_arn = Variable.get("ASSUME_ROLE_READ_ARN", default_var=None)
    return ingest_collection(
        dataset=config.get("dataset"), 
        data_type=config.get("data_type"),
        role_arn=role_arn
    )

def generate_collection():
    with TaskGroup(**group_kwgs) as collection_grp:
        run_generate_collection = PythonOperator(
            task_id="generate_collection",
            python_callable=generate_collection_task,
            trigger_rule=TriggerRule.ONE_SUCCESS
        )

        run_generate_collection

        return collection_grp
