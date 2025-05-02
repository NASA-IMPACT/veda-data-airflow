"""
Builds a DAG for each collection (indicated by a .json file) in the <BUCKET>/collections/ folder.
These DAGs are used to discover and ingest items for each collection.
"""

from airflow.models.variable import Variable
from typing import Dict, List, Optional

from veda_data_pipeline.veda_discover_pipeline import get_discover_dag
from veda_data_pipeline.veda_vector_pipeline import get_ingest_vector_dag
from veda_data_pipeline.veda_pyarc2stac_pipeline import get_ingest_pyarc2stac_dag

def filter_configs_by_dag(
        collection_configs: List[Dict[str, int]], 
        dag: Optional[str] = "veda_discover"
    ) -> List[Dict[str, int]]:
    """
    Args:
        collection_configs: The list of configs to filter
        dag: The DAG name to filter for (default is veda_discover).

    Returns:
        A new list containing only the collection configs that match the filter criteria.
    """
    
    filtered_configs = []
    for c in collection_configs:
        if c.get("schedule", None) and c.get("dag", "veda_discover") == dag:
            filtered_configs.append(c)
    return filtered_configs

def generate_dags():
    import boto3
    import json
    from botocore.exceptions import ClientError, NoCredentialsError

    from pathlib import Path

    airflow_vars = Variable.get("aws_dags_variables")
    airflow_vars_json = json.loads(airflow_vars)
    bucket = airflow_vars_json.get("EVENT_BUCKET")

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
        if type(collection_configs) is dict:
            collection_configs = [collection_configs]

        # Filter and handle collection configs by DAG

        # veda_discover
        scheduled_discovery_configs = filter_configs_by_dag(collection_configs, "veda_discover")
        for idx, discovery_config in enumerate(scheduled_discovery_configs):
            id = f"discover-{file_name}"
            if idx > 0:
                id = f"{id}-{idx}"
            get_discover_dag(
                id=id, event=discovery_config
            )

        # veda_vector_ingest
        scheduled_vector_configs = filter_configs_by_dag(collection_configs, "veda_ingest_vector")

        for idx, vector_config in enumerate(scheduled_vector_configs):
            id = f"vector-{file_name}"
            if idx > 0:
                id = f"{id}-{idx}"
            get_ingest_vector_dag(
                id=id, event=vector_config
            )

        # veda_pyarc2stac_ingest
        scheduled_pyarcstac_configs = filter_configs_by_dag(collection_configs, "veda_pyarc2stac_ingest")
        
        for idx, vector_config in enumerate(scheduled_pyarcstac_configs):
            id = f"pyarc2stac-{vector_config['id']}"
            get_ingest_pyarc2stac_dag(
                id=id, event=vector_config
            )

generate_dags()
