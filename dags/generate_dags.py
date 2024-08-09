"""
Builds a DAG for each collection (indicated by a .json file) in the <BUCKET>/collections/ folder.
These DAGs are used to discover and ingest items for each collection.
"""

from airflow.models.variable import Variable

from veda_data_pipeline.veda_discover_pipeline import get_discover_dag


def generate_dags():
    import boto3
    import json

    from pathlib import Path


    mwaa_stac_conf = Variable.get("MWAA_STACK_CONF", deserialize_json=True)
    bucket = mwaa_stac_conf["EVENT_BUCKET"]

    client = boto3.client("s3")
    response = client.list_objects_v2(Bucket=bucket, Prefix="collections/")

    for file_ in response.get("Contents", []):
        key = file_["Key"]
        if key.endswith("/"):
            continue
        file_name = Path(key).stem
        result = client.get_object(Bucket=bucket, Key=key)
        discovery_configs = result["Body"].read().decode()
        discovery_configs = json.loads(discovery_configs)

        # Allow the file content to be either one config or a list of configs
        if type(discovery_configs) is dict:
            discovery_configs = [discovery_configs]
        scheduled_discovery_configs = [
            discovery_config
                for discovery_config in discovery_configs
                    if discovery_config.get("schedule")
            ]
        for idx, discovery_config in enumerate(scheduled_discovery_configs):
            id = f"discover-{file_name}"
            if idx > 0:
                id = f"{id}-{idx}"
            get_discover_dag(
                id=id, event=discovery_config
            )


generate_dags()
