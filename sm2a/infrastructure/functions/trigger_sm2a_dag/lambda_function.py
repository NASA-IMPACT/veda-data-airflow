import http.client
import json
import os
import uuid
from base64 import b64encode

import boto3


def lambda_handler(event, context):
    secrets_client = boto3.client("secretsmanager")
    sm2a_secret_manager_name = os.getenv("SM2A_SECRET_MANAGER_NAME")

    # Extract payload from CLI invocation

    payload = event.get("payload", {})

    # Get DAG name from payload
    dag_name = payload.get("dag_name")

    # Get custom configuration from payload if provided
    custom_config = payload.get("conf", {})

    try:
        secret_response = secrets_client.get_secret_value(
            SecretId=sm2a_secret_manager_name
        )
        secret_data = json.loads(secret_response["SecretString"])
        sm2a_domain_name = secret_data["airflow_webserver_url"]
        username = secret_data["airflow_admin_username"]
        password = secret_data["airflow_admin_password"]
        data = {
            "conf": custom_config,
            "dag_run_id": f"{dag_name}-{uuid.uuid4()}",
            "note": "Run from direct invocation",
        }
        # Create the HTTP connection
        conn = http.client.HTTPSConnection(sm2a_domain_name)
    except Exception as ex:
        return {"statusCode": 500, "body": json.dumps(f"Error: {ex}")}

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
