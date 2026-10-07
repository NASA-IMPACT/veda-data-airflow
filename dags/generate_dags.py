"""
Builds a DAG for each collection (indicated by a .json file) in the
<BUCKET>/collections/folder.

These DAGs are used to discover and ingest items for each collection.
"""

from airflow.sdk import Variable
from veda_data_pipeline.helpers.veda_wmts2stac_update_pipeline import (
    get_ingest_wmts2stac_dag_config,
)
from veda_data_pipeline.veda_discover_pipeline import get_discover_dag
from veda_data_pipeline.veda_pyarc2stac_pipeline import get_ingest_pyarc2stac_dag
from veda_data_pipeline.veda_vector_pipeline import get_ingest_vector_dag
from veda_data_pipeline.veda_wmts2stac_update_pipeline import get_ingest_wmts2stac_dag

dag_generators = {
    "veda_discover": get_discover_dag,
    "veda_ingest_vector": get_ingest_vector_dag,
    "veda_pyarc2stac_ingest": get_ingest_pyarc2stac_dag,
    "veda_wmts2stac_ingest": get_ingest_wmts2stac_dag,
}

# preserve DAG history
dag_names = {
    "veda_discover": "discover",
    "veda_ingest_vector": "vector",
    "veda_pyarc2stac_ingest": "pyarc2stac",
    "veda_wmts2stac_ingest": "wmts2stac",
}


def generate_dags():
    import json
    from pathlib import Path

    import boto3
    from botocore.exceptions import ClientError, NoCredentialsError

    try:
        bucket = Variable.get("EVENT_BUCKET")
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

        for c in collection_configs:
            if c.get("schedule", None) and (dag := c.get("dag", "veda_discover")):
                if id := c.get("id"):
                    # use id for DAG name if provided, otherwise default to file name
                    file_name = id
                try:
                    dag_generators[dag](id=f"{dag_names[dag]}-{file_name}", event=c)
                except KeyError:
                    continue  # configured DAG not present in current environment


generate_dags()
# create default DAGs (no config or schedule)
get_ingest_vector_dag(id="veda_ingest_vector", event={})
get_discover_dag(id="veda_discover", event={})
get_ingest_wmts2stac_dag(id="veda_wmts2stac", event=get_ingest_wmts2stac_dag_config)
