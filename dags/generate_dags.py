"""
Builds a DAG for each collection (indicated by a .json file) in the <BUCKET>/collections/ folder.
These DAGs are used to discover and ingest items for each collection.
"""

from airflow.models.variable import Variable
from typing import Dict, List, Optional

from veda_data_pipeline.veda_discover_pipeline import get_discover_dag
from veda_data_pipeline.veda_vector_pipeline import get_ingest_vector_dag
from veda_data_pipeline.veda_pyarc2stac_pipeline import get_ingest_pyarc2stac_dag


def schedule_dags_by_config(
        dag_configs: Dict[str, tuple],
        collection_configs: List[Dict[str, int]],
        file_name: str 
    ) -> None:
    """
    Schedule Airflow DAGs for each collection config that includes a `schedule`.

    Args:
        dag_configs: mapping of dag_key -> (builder_fn, id_prefix)
        collection_configs: list of config dicts, each may include:
            - "dag": which key to use from dag_configs (defaults to "veda_discover")
            - "schedule": cron or schedule specifier (must be present to schedule)
            - "id": a unique identifier for the config (used by pyarc2stac)
            - other fields passed through as `event`
        file_name: filename stem used when generating each non-pyarc2stac task_id

    Outputs:
        DAGs based on the provided collection configurations. Operates on each entry in the .json file.
    """

    for idx, collection in enumerate(collection_configs):
        if not collection.get("schedule"):
            continue
        
        # Retrieves the function name from dag_configs
        dag_builder= dag_configs[collection.get("dag", "veda_discover")]

        name = (dag_builder.__name__).split('_')[-2]
        id = f"{name}-{collection['collection']}"

        dag_builder(id=id, event=collection)



def generate_dags():
    import boto3
    import json
    from botocore.exceptions import ClientError, NoCredentialsError

    from pathlib import Path

    airflow_vars = Variable.get("aws_dags_variables")
    airflow_vars_json = json.loads(airflow_vars)
    bucket = airflow_vars_json.get("EVENT_BUCKET")

    '''Define the mapping of DAG builders to their respective keys and prefixes
    The key values (e.g., veda_discover) are located as a key value pair in the AWS S3 bucket under the collections/ folder in the .json file.
    The mapping functions are located in the /veda_data_pipeline 
    The naming ID (e.g., discover, vector, pyarc2stac) is taken from the key value in dag_configs
    '''

    dag_configs = {
        "veda_discover":          get_discover_dag,
        "veda_ingest_vector":     get_ingest_vector_dag,
        "veda_pyarc2stac_ingest": get_ingest_pyarc2stac_dag,
    }


    try:
        client = boto3.client("s3")
        response = client.list_objects_v2(Bucket=bucket, Prefix="collections/")
    except ClientError as e:
        # Handle general AWS service errors (e.g., wrong bucket name)
        print(f"ClientError: {e}")
        return
    except NoCredentialsError:
        # Handle missing credentials
        print("Credentials not found.")
        return
    except Exception as ex:
        print(f"An unexpected error occurred: {ex}")
        return
    for file_ in response.get("Contents", []):
        key = file_["Key"]
        if key.endswith("/"):
            continue
        file_name = Path(key).stem
        result = client.get_object(Bucket=bucket, Key=key)
        collection_configs = result["Body"].read().decode()
        collection_configs = json.loads(collection_configs)

        # Allow the file content to be either one config or a list of configs
        collection_configs = [collection_configs] if type(collection_configs) is dict else collection_configs

        schedule_dags_by_config(dag_configs, 
                                collection_configs, 
                                file_name
                                )


generate_dags()
