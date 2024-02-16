import logging
import os
from getpass import getuser
from typing import Union

import requests
import src.airflow_helpers as airflow_helpers
import src.auth as auth
import src.config as config
import src.schemas as schemas
from src.collection_publisher import CollectionPublisher, ItemPublisher, Publisher

from fastapi import Body, Depends, FastAPI, HTTPException
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

settings = (
    config.Settings()
    if os.environ.get("NO_PYDANTIC_SSM_SETTINGS")
    else config.Settings.from_ssm(
        stack=os.environ.get(
            "STACK", f"veda-stac-ingestion-system-{os.environ.get('STAGE', getuser())}"
        ),
    )
)

logger = logging.getLogger(__name__)

collection_publisher = CollectionPublisher()
item_publisher = ItemPublisher()
publisher = Publisher()

# Subapp for managing Processes and DAG executions (workflows)

workflows_app = FastAPI(
    title="VEDA Workflows API",
    license_info={
        "name": "Apache 2.0",
        "url": "https://www.apache.org/licenses/LICENSE-2.0.html",
    },
    contact={"url": "https://github.com/NASA-IMPACT/veda-backend"},
)


# "Datasets" interface (collections + item ingests from one input)

@workflows_app.post(
    "/dataset/validate",
    tags=["Dataset"],
    dependencies=[Depends(auth.get_username)],
)
def validate_dataset(dataset: schemas.COGDataset):
    # for all sample files in dataset, test access using raster /validate endpoint
    for sample in dataset.sample_files:
        url = f"{settings.raster_url}/cog/validate?url={sample}"
        try:
            response = requests.get(url)
            if response.status_code != 200:
                raise HTTPException(
                    status_code=response.status_code,
                    detail=(f"Unable to validate dataset: {response.text}"),
                )
        except Exception as e:
            raise HTTPException(
                status_code=422,
                detail=(f"Sample file {sample} is an invalid COG: {e}"),
            )
    return {
        f"Dataset metadata is valid and ready to be published - {dataset.collection}"
    }


@workflows_app.post(
    "/dataset/publish", tags=["Dataset"], dependencies=[Depends(auth.get_username)]
)
async def publish_dataset(
    dataset: Union[schemas.ZarrDataset, schemas.COGDataset] = Body(
        ..., discriminator="data_type"
    )
):
    # Construct and load collection
    collection_data = publisher.generate_stac(dataset, dataset.data_type or "cog")
    collection = schemas.DashboardCollection.parse_obj(collection_data)
    collection_publisher.ingest(collection)

    # TODO improve typing
    return_dict = {
        "message": f"Successfully published collection: {dataset.collection}."
    }

    if dataset.data_type == schemas.DataType.cog:
        workflow_runs = []
        for discovery in dataset.discovery_items:
            discovery.collection = dataset.collection
            response = await start_discovery_workflow_execution(discovery)
            workflow_runs.append(response.id)
        if workflow_runs:
            return_dict["message"] += f" {len(workflow_runs)} workflows initiated."  # type: ignore
            return_dict["workflows_ids"] = workflow_runs  # type: ignore

    return return_dict



@workflows_app.post(
    "/discovery",
    response_model=schemas.WorkflowExecutionResponse,
    tags=["Workflow-Executions"],
    status_code=201,
    dependencies=[Depends(auth.get_username)],
)
async def start_discovery_workflow_execution(
    input=Body(..., discriminator="discovery"),
) -> schemas.WorkflowExecutionResponse:
    """
    Triggers the ingestion workflow
    """
    return airflow_helpers.trigger_discover(input)


@workflows_app.get(
    "/discovery-executions/{workflow_execution_id}",
    response_model=Union[schemas.ExecutionResponse, schemas.WorkflowExecutionResponse],
    tags=["Workflow-Executions"],
    dependencies=[Depends(auth.get_username)],
)
async def get_discovery_workflow_execution_status(
    workflow_execution_id: str,
) -> Union[schemas.ExecutionResponse, schemas.WorkflowExecutionResponse]:
    """
    Returns the status of the workflow execution
    """
    return airflow_helpers.get_status(workflow_execution_id)


@workflows_app.get(
    "/list-workflows",
    tags=["Workflow-Executions"],
    dependencies=[Depends(auth.get_username)],
)
async def get_workflow_list() -> Union[
    schemas.ExecutionResponse, schemas.WorkflowExecutionResponse
]:
    """
    Returns the status of the workflow execution
    """
    return airflow_helpers.list_dags()


@workflows_app.post(
    "/cli-input",
    tags=["Admin"],
    dependencies=[Depends(auth.get_username)],
)
async def send_cli_command(cli_command: str):
    return airflow_helpers.send_cli_command(cli_command)


# exception handling
@workflows_app.exception_handler(RequestValidationError)
async def validation_exception_handler(request, exc):
    return JSONResponse(str(exc), status_code=422)
