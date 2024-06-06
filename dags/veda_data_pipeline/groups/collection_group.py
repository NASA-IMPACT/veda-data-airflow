from typing import Any, Dict

import requests
from airflow.models.variable import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.task_group import TaskGroup
from veda_data_pipeline.utils.collection_generation import GenerateCollection
from veda_data_pipeline.utils.submit_stac import submission_handler

generator = GenerateCollection()


def check_collection_exists(endpoint: str, collection_id: str):
    """
    Check if a collection exists in the STAC catalog

    Args:
        endpoint (str): STAC catalog endpoint
        collection_id (str): collection id
    """
    response = requests.get(f"{endpoint}/collections/{collection_id}")
    return (
        "Collection.existing_collection"
        if (response.status_code == 200)
        else "Collection.generate_collection"
    )


def ingest_collection(dataset_config: Dict[str, Any], role_arn: str = None):
    """
    Ingest a collection into the STAC catalog

    Args:
        dataset (Dict[str, Any]): dataset dictionary (JSON)
        role_arn (str): role arn for Zarr collection generation
    """
    collection = generator.generate_stac(
        dataset_config=dataset_config, role_arn=role_arn
    )

    return submission_handler(
        event=collection,
        endpoint="/collections",
        cognito_app_secret=Variable.get("COGNITO_APP_SECRET"),
        stac_ingestor_api_url=Variable.get("STAC_INGESTOR_API_URL"),
    )


def check_collection_exists_task(ti):
    config = ti.dag_run.conf
    return check_collection_exists(
        endpoint=Variable.get("STAC_URL", default_var=None),
        collection_id=config.get("collection"),
    )


def generate_collection_task(ti):
    config = ti.dag_run.conf
    role_arn = Variable.get("ASSUME_ROLE_READ_ARN", default_var=None)
    return ingest_collection(
        dataset_config=config,
        role_arn=role_arn,
    )


group_kwgs = {"group_id": "Collection", "tooltip": "Collection"}


def collection_task_group():
    with TaskGroup(**group_kwgs) as collection_task_grp:
        check_collection = BranchPythonOperator(
            task_id="check_collection_exists",
            python_callable=check_collection_exists_task,
        )

        generate_collection = PythonOperator(
            task_id="generate_collection", python_callable=generate_collection_task
        )

        existing_collection = EmptyOperator(task_id="existing_collection")

        (check_collection >> [existing_collection, generate_collection])

        return collection_task_grp
