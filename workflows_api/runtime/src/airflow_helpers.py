import base64
from typing import Dict
from uuid import uuid4

import boto3
import requests
import json
from fastapi import HTTPException

from src.config import settings
from src.schemas import WorkflowExecutionResponse, Status, ListWorkflowsResponse


def cli_input(cli_string: str) -> Dict:
    """
    Pass a command directly to the CLI. Requires auth.
    """
    if not (mwaa_env := settings.mwaa_env):
        raise HTTPException(status_code=400, detail="MWAA environment not set")

    airflow_client = boto3.client("mwaa")
    mwaa_cli_token = airflow_client.create_cli_token(Name=mwaa_env)

    mwaa_webserver_hostname = (
        f"https://{mwaa_cli_token['WebServerHostname']}/aws_mwaa/cli"
    )

    mwaa_response = requests.post(
        mwaa_webserver_hostname,
        headers={
            "Authorization": "Bearer " + mwaa_cli_token["CliToken"],
            "Content-Type": "application/json",
        },
        data=cli_string,
    )
    if mwaa_response.raise_for_status():
        raise Exception(
            f"Failed to trigger airflow: {mwaa_response.status_code} "
            f"{mwaa_response.text}"
        )
    else:
        return WorkflowExecutionResponse(**mwaa_response)


def trigger_discover(input: Dict) -> Dict:
    if not (mwaa_env := settings.mwaa_env):
        raise HTTPException(status_code=400, detail="MWAA environment not set")

    airflow_client = boto3.client("mwaa")
    mwaa_cli_token = airflow_client.create_cli_token(Name=mwaa_env)

    mwaa_webserver_hostname = (
        f"https://{mwaa_cli_token['WebServerHostname']}/aws_mwaa/cli"
    )

    unique_key = str(uuid4())
    raw_data = f"dags trigger veda_discover --conf '{json.dumps(input)}' -r {unique_key}"
    mwaa_response = requests.post(
        mwaa_webserver_hostname,
        headers={
            "Authorization": "Bearer " + mwaa_cli_token["CliToken"],
            "Content-Type": "application/json",
        },
        data=raw_data,
    )
    if mwaa_response.raise_for_status():
        raise Exception(
            f"Failed to trigger airflow: {mwaa_response.status_code} "
            f"{mwaa_response.text}"
        )
    else:
        return WorkflowExecutionResponse(
            **{
                "id": unique_key,
                "status": Status.started,
            }
        )


def list_dags() -> Dict:
    if not (mwaa_env := settings.mwaa_env):
        raise HTTPException(status_code=400, detail="MWAA environment not set")

    airflow_client = boto3.client("mwaa")
    mwaa_cli_token = airflow_client.create_cli_token(Name=mwaa_env)

    mwaa_webserver_hostname = (
        f"https://{mwaa_cli_token['WebServerHostname']}/aws_mwaa/cli"
    )

    raw_data = "dags list"
    mwaa_response = requests.post(
        mwaa_webserver_hostname,
        headers={
            "Authorization": "Bearer " + mwaa_cli_token["CliToken"],
            "Content-Type": "application/json",
        },
        data=raw_data,
    )

    if mwaa_response.raise_for_status():
        raise Exception(
            f"Failed to trigger airflow: {mwaa_response.status_code} "
            f"{mwaa_response.text}"
        )
    else:
        # decode base64 encoded string output and parse the text
        decoded_response = base64.b64decode(mwaa_response.json()["stdout"]).decode("utf-8")
        dags_response = []
        for item in decoded_response.split("\n")[2:]:
            if (row := item.replace(" ", "")) == "":
                continue
            
            columns = row.split("|")
            dags_response.append({
                "dag_id": columns[0],
                "filepath":columns[1],
                "owner": columns[2],
                "paused": columns[3]
            })
        
        return ListWorkflowsResponse(
            dags= dags_response
        )


def get_status(dag_run_id: str) -> Dict:
    """
    Get the status of a veda_discover workflow execution by dag_id
    """
    if not (mwaa_env := settings.mwaa_env):
        raise HTTPException(status_code=400, detail="MWAA environment not set")

    airflow_client = boto3.client("mwaa")
    mwaa_cli_token = airflow_client.create_cli_token(Name=mwaa_env)

    mwaa_webserver_hostname = (
        f"https://{mwaa_cli_token['WebServerHostname']}/aws_mwaa/cli"
    )

    raw_data = "dags list-runs -d veda_discover"
    mwaa_response = requests.post(
        mwaa_webserver_hostname,
        headers={
            "Authorization": "Bearer " + mwaa_cli_token["CliToken"],
            "Content-Type": "application/json",
        },
        data=raw_data,
    )
    decoded_response = base64.b64decode(mwaa_response.json()["stdout"]).decode("utf8")
    rows = decoded_response.split("\n")

    try:
        matched_row = next(row for row in rows if dag_run_id in row)
    except StopIteration:
        raise Exception(f"Failed to find dag run id: {dag_run_id}")

    columns = matched_row.split("|")
    status = columns[2].strip()

    # Statuses in Airflow differ slightly from our own, so we convert them here
    if status == "success":
        run_status = Status.succeeded
    elif status == "failed":
        run_status = Status.failed
    elif status == "running":
        run_status = Status.started
    elif status == "queued":
        run_status = Status.queued

    return WorkflowExecutionResponse(
        **{
            "id": dag_run_id,
            "status": run_status,
        }
    )
