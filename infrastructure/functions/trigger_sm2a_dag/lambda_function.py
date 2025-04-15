import http.client
import json
import os
import uuid
import logging
from base64 import b64encode

import boto3

# Set up logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):

    secrets_client = boto3.client("secretsmanager")
    sm2a_secret_manager_name = os.getenv("SM2A_SECRET_MANAGER_NAME")


    # Extract payload from CLI invocation
    payload = event.get("payload", {})

    # Get DAG name from payload
    dag_name = payload.get("dag_name")
    if not dag_name:
        error_msg = "Missing required parameter: dag_name"
        logger.error(error_msg)
        return {"statusCode": 400, "body": json.dumps({"error": error_msg})}

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

        dag_run_id = f"{dag_name}-{uuid.uuid4()}"

        data = {
            "conf": custom_config,
            "dag_run_id": dag_run_id,
            "note": "Run from direct invocation",
        }

        # Create the HTTP connection
        conn = http.client.HTTPSConnection(sm2a_domain_name)
    except json.JSONDecodeError as ex:
        error_msg = f"Error parsing secret data: {str(ex)}"
        logger.error(error_msg, exc_info=True)
        return {"statusCode": 500, "body": json.dumps({"error": error_msg})}
    except Exception as ex:
        error_msg = f"Error setting up connection: {str(ex)}"
        logger.error(error_msg, exc_info=True)
        return {"statusCode": 500, "body": json.dumps({"error": error_msg})}

    headers = {
        "Content-Type": "application/json",
        "Authorization": "Basic "
                         + b64encode(f"{username}:{password}".encode()).decode(),
    }

    try:
        conn.request("POST", f"/api/v1/dags/{dag_name}/dagRuns", json.dumps(data), headers)

        # Get the response
        response = conn.getresponse()
        response_data = response.read().decode()

        # Log response
        logger.info(f"Response status: {response.status}, data: {response_data}")

        # Close the connection
        conn.close()

        if response.status >= 300:
            logger.error(f"Error from Airflow API: Status {response.status}, Response: {response_data}")

        return {"statusCode": response.status, "body": response_data}
    except Exception as ex:
        error_msg = f"Error during API request: {str(ex)}"
        logger.error(error_msg, exc_info=True)
        return {"statusCode": 500, "body": json.dumps({"error": error_msg})}
