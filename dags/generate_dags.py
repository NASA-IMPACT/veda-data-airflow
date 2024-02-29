"""
Builds a DAG for each collection (indicated by a .json file) in the <BUCKET>/collections/ folder.
These DAGs are used to discover and ingest items for each collection.
"""
from airflow.models.variable import Variable


from veda_data_pipeline.veda_discover_pipeline import get_discover_dag


def generate_dags():
    import boto3
    import json

    MWAA_STAC_CONF = Variable.get("MWAA_STACK_CONF", deserialize_json=True)
    bucket = MWAA_STAC_CONF["EVENT_BUCKET"]

    client = boto3.client('s3')
    response = client.list_objects_v2(Bucket=bucket, Prefix="collections/")

    for file_ in response["Contents"]:
        key = file_["Key"]
        if key.endswith("/"):
            continue
        result = client.get_object(Bucket=bucket, Key=key)
        collection = result["Body"].read().decode()
        collection = json.loads(collection)
        get_discover_dag(id=collection["id"], event=collection)
        

generate_dags()
