import http.client
import json
import os
import uuid
from base64 import b64encode

import boto3


def lambda_handler(event, context):
    secrets_client = boto3.client("secretsmanager")
    sm2a_secret_manager_name = os.getenv("SM2A_SECRET_MANAGER_NAME")
    dag_name = os.getenv("TARGET_DAG_ID")
    storage_bucket = os.getenv("STORAGE_BUCKET")
    try:
        secret_response = secrets_client.get_secret_value(
            SecretId=sm2a_secret_manager_name
        )
        secret_data = json.loads(secret_response["SecretString"])
        sm2a_domain_name = secret_data["airflow_webserver_url"]
        username = secret_data["airflow_admin_username"]
        password = secret_data["airflow_admin_password"]
        record = event["Records"][0]
        print(record)
        # Create the HTTP connection
        conn = http.client.HTTPSConnection(sm2a_domain_name)
    except Exception as ex:
        return {"statusCode": 500, "body": json.dumps(f"Error: {ex}")}

    s3_event_key = record["s3"]["object"]["key"]
    s3_filename_target = os.path.split(s3_event_key)[-1]
    s3_filename_no_ext = os.path.splitext(s3_filename_target)[0]
    bucket_key_prefix = os.path.dirname(s3_event_key)
    data = {
        "conf": {
            "discovery": "s3",
            "collection": s3_filename_no_ext,
            "prefix": bucket_key_prefix,
            "bucket": storage_bucket,
            "filename_regex": f"^(.*){s3_filename_target}$",
            "vector_eis": True,
        },
        "dag_run_id": f"{dag_name}-{uuid.uuid4()}",
        "note": "Run from S3 Event bridge",
    }
    headers = {
        "Content-Type": "application/json",
        "Authorization": "Basic "
        + b64encode(f"{username}:{password}".encode()).decode(),
    }

    conn.request("POST", f"/api/v1/dags/{dag_name}/dagRuns", json.dumps(data), headers)

    # Get the response
    response = conn.getresponse()
    response_data = response.read()

    # Close the connection
    conn.close()

    return {"statusCode": response.status, "body": response_data.decode()}
