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
        dag_builders: Dict[str, callable],
        dag_name_mapping: Dict[str, callable],
        collection_configs: List[Dict[str, int]],
        file_name: str 
    ) -> List[Dict[str, int]]:
    """
    Schedule Airflow DAGs for each collection config that includes a `schedule`.

    For every config dict in `collection_configs` with a non-None `"schedule"`:
    1. Look up its DAG key (`"dag"`, defaulting to `"veda_discover"`).
    2. Build a unique `task_id` by combining the prefix from `dag_name_mapping`
       with `file_name`
    3. Invoke the corresponding builder function from `dag_builders` with:
          builder(id=task_id, event=config)

    Args:
        dag_builders (Dict[str, callable]):
            Maps each DAG key (e.g. "veda_discover") to its factory function
            (e.g. `get_discover_dag`). Each factory accepts `id` and `event`.
        dag_name_mapping (Dict[str, str]):
            Maps the same DAG keys to short ID prefixes 
            (e.g. "discover", "vector", "pyarc2stac").
        collection_configs (List[Dict[str, int]]):
            List of configuration dicts loaded from JSON. Each may include:
              - `"dag"`: which DAG to use
              - `"schedule"`: cron or schedule specifier (must be present to schedule)
              - other fields passed through as `event`
        file_name (str):
            Base name (JSON filename stem) used when generating each `task_id`.

    Output:
        DAG for each collection config that includes a `schedule`.

    """
    
    for idx,collection in enumerate(collection_configs):
        if collection.get("schedule", None):

            function_to_call = collection.get("dag", "veda_discover") #To align with previous code (which required veda_discover as default)
            id = f"{dag_name_mapping[function_to_call]}-{file_name}"

            is_pyarc = dag_name_mapping[function_to_call] == "pyarc2stac" 
            #Name pyarc2stac DAGs with the collection ID instead of the file name to assist with interpretibility in the Airflow UI
            id = (
                f"{dag_name_mapping[function_to_call]}-{collection['id']}"
                if is_pyarc
                else (f"{id}-{idx}" if idx > 0 else id)
            )

            dag_builders[function_to_call](
                id=id, event=collection
            )


def generate_dags():
    import boto3
    import json
    from botocore.exceptions import ClientError, NoCredentialsError

    from pathlib import Path

    airflow_vars = Variable.get("aws_dags_variables")
    airflow_vars_json = json.loads(airflow_vars)
    bucket = airflow_vars_json.get("EVENT_BUCKET")

    dag_builders = {
        # Mapping of DAG keys to their builder functions
        "veda_discover":          get_discover_dag,
        "veda_ingest_vector":     get_ingest_vector_dag,
        "veda_pyarc2stac_ingest": get_ingest_pyarc2stac_dag,
    }

    dag_name_mapping = {
        # ID name mapping during DAG creation
        "veda_discover":          "discover",
        "veda_ingest_vector":     "vector",
        "veda_pyarc2stac_ingest": "pyarc2stac",
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

        schedule_dags_by_config(dag_builders, 
                                dag_name_mapping, 
                                collection_configs, 
                                file_name
                                )


generate_dags()
