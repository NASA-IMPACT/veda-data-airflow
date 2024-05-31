from typing import Union

import requests
import src.airflow_helpers as airflow_helpers
import src.auth as auth
import src.config as config
import src.schemas as schemas

from src.monitoring import LoggerRouteHandler, logger, metrics, tracer
from aws_lambda_powertools.metrics import MetricUnit

from fastapi import Body, Depends, FastAPI, HTTPException, APIRouter
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from starlette.requests import Request


settings = config.Settings()

# App for managing Processes and DAG executions (workflows)

workflows_app = FastAPI(
    title="VEDA Workflows API",
    license_info={
        "name": "Apache 2.0",
        "url": "https://www.apache.org/licenses/LICENSE-2.0.html",
    },
    contact={"url": "https://github.com/NASA-IMPACT/veda-backend"},
    root_path=settings.workflow_root_path,
    openapi_url="/openapi.json",
    docs_url="/docs",
    swagger_ui_init_oauth={
        "appName": "Cognito",
        "clientId": settings.client_id,
        "usePkceWithAuthorizationCodeGrant": True,
    },
    router=APIRouter(route_class=LoggerRouteHandler),
)


# "Datasets" interface (collections + item ingests from one input)


@workflows_app.post(
    "/dataset/validate",
    tags=["Dataset"],
    dependencies=[Depends(auth.validated_token)],
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
    "/discovery",
    response_model=schemas.WorkflowExecutionResponse,
    tags=["Workflow-Executions"],
    status_code=201,
    dependencies=[Depends(auth.validated_token)],
)
async def start_discovery_workflow_execution(
    input: Union[schemas.S3Input, schemas.CmrInput] = Body(
        ..., discriminator="discovery"
    ),
) -> schemas.WorkflowExecutionResponse:
    """
    Triggers the ingestion workflow
    """
    return airflow_helpers.trigger_discover(jsonable_encoder(input))


@workflows_app.get(
    "/discovery-executions/{workflow_execution_id}",
    response_model=Union[schemas.ExecutionResponse, schemas.WorkflowExecutionResponse],
    tags=["Workflow-Executions"],
    dependencies=[Depends(auth.validated_token)],
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
    dependencies=[Depends(auth.validated_token)],
)
async def get_workflow_list() -> (
    Union[schemas.ExecutionResponse, schemas.WorkflowExecutionResponse]
):
    """
    Returns the status of the workflow execution
    """
    return airflow_helpers.list_dags()


@workflows_app.post(
    "/cli-input",
    tags=["Admin"],
    dependencies=[Depends(auth.validated_token)],
)
async def send_cli_command(cli_command: str):
    return airflow_helpers.send_cli_command(cli_command)


# If the correlation header is used in the UI, we can analyze traces that originate from a given user or client
@workflows_app.middleware("http")
async def add_correlation_id(request: Request, call_next):
    """Add correlation ids to all requests and subsequent logs/traces"""
    # Get correlation id from X-Correlation-Id header if provided
    corr_id = request.headers.get("x-correlation-id")
    if not corr_id:
        try:
            # If empty, use request id from aws context
            corr_id = request.scope["aws.context"].aws_request_id
        except KeyError:
            # If empty, use uuid
            corr_id = "local"

    # Add correlation id to logs
    logger.set_correlation_id(corr_id)

    # Add correlation id to traces
    tracer.put_annotation(key="correlation_id", value=corr_id)

    response = await tracer.capture_method(call_next)(request)
    # Return correlation header in response
    response.headers["X-Correlation-Id"] = corr_id
    logger.info("Request completed")
    return response


@workflows_app.get("/auth/me", tags=["Auth"])
def who_am_i(claims=Depends(auth.validated_token)):
    """
    Return claims for the provided JWT
    """
    return claims


# exception handling
@workflows_app.exception_handler(RequestValidationError)
async def validation_exception_handler(request, exc):
    metrics.add_metric(name="ValidationErrors", unit=MetricUnit.Count, value=1)
    return JSONResponse(str(exc), status_code=422)


@workflows_app.exception_handler(Exception)
async def general_exception_handler(request, err):
    """Handle exceptions that aren't caught elsewhere"""
    metrics.add_metric(name="UnhandledExceptions", unit=MetricUnit.Count, value=1)
    logger.exception(f"Unhandled exception: {err}")
    return JSONResponse(status_code=500, content={"detail": "Internal Server Error"})
