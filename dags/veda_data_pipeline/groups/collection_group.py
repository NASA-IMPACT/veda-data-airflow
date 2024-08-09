import requests
from airflow.models.variable import Variable
from airflow.operators.python import PythonOperator
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


def ingest_collection_task(ti):
    """
    Ingest a collection into the STAC catalog

    Args:
        dataset (Dict[str, Any]): dataset dictionary (JSON)
        role_arn (str): role arn for Zarr collection generation
    """
    collection = ti.xcom_pull(task_ids='Collection.generate_collection')

    return submission_handler(
        event=collection,
        endpoint="/collections",
        cognito_app_secret=Variable.get("COGNITO_APP_SECRET"),
        stac_ingestor_api_url=Variable.get("STAC_INGESTOR_API_URL"),
    )


# NOTE unused, but useful for item ingests, since collections are a dependency for items
def check_collection_exists_task(ti):
    config = ti.dag_run.conf
    return check_collection_exists(
        endpoint=Variable.get("STAC_URL", default_var=None),
        collection_id=config.get("collection"),
    )


def generate_collection_task(ti):
    config = ti.dag_run.conf
    role_arn = Variable.get("ASSUME_ROLE_READ_ARN", default_var=None)

    # TODO it would be ideal if this also works with complete collections where provided - this would make the collection ingest more re-usable
    collection = generator.generate_stac(
        dataset_config=config, role_arn=role_arn
    )
    return collection



group_kwgs = {"group_id": "Collection", "tooltip": "Collection"}


def collection_task_group():
    with TaskGroup(**group_kwgs) as collection_task_grp:
        generate_collection = PythonOperator(
            task_id="generate_collection", python_callable=generate_collection_task
        )
        ingest_collection = PythonOperator(
            task_id="ingest_collection", python_callable=ingest_collection_task
        )
        generate_collection >> ingest_collection

        return collection_task_grp
