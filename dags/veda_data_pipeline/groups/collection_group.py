import requests
from airflow.models.variable import Variable
from airflow.utils.task_group import TaskGroup
from airflow.decorators import task
from veda_data_pipeline.utils.collection_generation import GenerateCollection
from veda_data_pipeline.utils.submit_stac import submission_handler


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





# NOTE unused, but useful for item ingests, since collections are a dependency for items
def check_collection_exists_task(ti):
    import json
    config = ti.dag_run.conf
    airflow_vars = Variable.get("aws_dags_variables")
    airflow_vars_json = json.loads(airflow_vars)
    stac_url = airflow_vars_json.get("STAC_URL")
    return check_collection_exists(
        endpoint=stac_url,
        collection_id=config.get("collection"),
    )




group_kwgs = {"group_id": "Collection", "tooltip": "Collection"}


def collection_task_group():
    with TaskGroup(**group_kwgs) as collection_task_grp:
        @task()
        def generate_collection_task(ti):

            config = ti.dag_run.conf
            airflow_vars_json = Variable.get("aws_dags_variables", deserialize_json=True)
            role_arn = airflow_vars_json.get("ASSUME_ROLE_READ_ARN")

            # TODO it would be ideal if this also works with complete collections where provided - this would make the collection ingest more re-usable
            generator = GenerateCollection()
            collection = generator.generate_stac(
                dataset_config=config, role_arn=role_arn
            )
            return collection

        @task()
        def ingest_collection_task(collection):
            """
            Ingest a collection into the STAC catalog

            Args:
                collection:

            """
            airflow_vars_json = Variable.get("aws_dags_variables", deserialize_json=True)
            cognito_app_secret = airflow_vars_json.get("COGNITO_APP_SECRET")
            stac_ingestor_api_url = airflow_vars_json.get("STAC_INGESTOR_API_URL")

            return submission_handler(
                event=collection,
                endpoint="/collections",
                cognito_app_secret=cognito_app_secret,
                stac_ingestor_api_url=stac_ingestor_api_url
            )

        collection = generate_collection_task()
        ingest_collection_task(collection)

        return collection_task_grp
