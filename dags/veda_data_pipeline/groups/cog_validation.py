from airflow.utils.task_group import TaskGroup
import os
import pandas as pd
import uuid
import rioxarray
from urllib.parse import urlparse
import great_expectations as gx
import boto3
from great_expectations.data_context.types.base import DataContextConfig
from great_expectations.core.batch import RuntimeBatchRequest
from airflow.decorators import task
import rasterio

group_kwgs = {"group_id": "COGValidation", "tooltip": "COG Validation"}
def geotiff_to_pandas(s3_input_raster):
    dataarray = rioxarray.open_rasterio(s3_input_raster, chuncks=True)
    dataarray.attrs.update({'crs': dataarray.rio.crs.to_proj4()})
    dataarray.attrs.update({'nodata': dataarray.rio.nodata})
    with rasterio.open(s3_input_raster, chuncks=True) as _f:
        data = _f.read()
    meta_df = pd.json_normalize(dataarray.attrs)
    _, y, x = dataarray.indexes.values()
    pandas_df = pd.concat([pd.DataFrame(data={'x': x}), pd.DataFrame(data={'y': y}),
                           pd.DataFrame(data={'data': data.ravel()}), meta_df], axis=1)
    return pandas_df


base_path = "/tmp"
data_dir = os.path.join(base_path, "data")
ge_root_dir = os.path.join(base_path, "great_expectations")
data_context_config = DataContextConfig(
    **{
        "config_version": 3.0,
        "datasources": {
            "my_datasource": {
                "module_name": "great_expectations.datasource",
                "data_connectors": {
                    "default_inferred_data_connector_name": {
                        "default_regex": {
                            "group_names": ["data_asset_name"],
                            "pattern": "(.*)",
                        },
                        "base_directory": data_dir,
                        "module_name": "great_expectations.datasource.data_connector",
                        "class_name": "InferredAssetFilesystemDataConnector",
                    },
                    "default_runtime_data_connector_name": {
                        "batch_identifiers": ["default_identifier_name"],
                        "module_name": "great_expectations.datasource.data_connector",
                        "class_name": "RuntimeDataConnector",
                    },
                },
                "execution_engine": {
                    "module_name": "great_expectations.execution_engine",
                    "class_name": "PandasExecutionEngine",
                },
                "class_name": "Datasource",
            }
        },
        "config_variables_file_path": os.path.join(ge_root_dir, "uncommitted", "config_variables.yml"),
        "stores": {
            "expectations_store": {
                "class_name": "ExpectationsStore",
                "store_backend": {
                    "class_name": "TupleFilesystemStoreBackend",
                    "base_directory": os.path.join(ge_root_dir, "expectations"),
                },
            },
            "validations_store": {
                "class_name": "ValidationsStore",
                "store_backend": {
                    "class_name": "TupleFilesystemStoreBackend",
                    "base_directory": os.path.join(ge_root_dir, "uncommitted", "validations"),
                },
            },
            "evaluation_parameter_store": {"class_name": "EvaluationParameterStore"},
            "checkpoint_store": {
                "class_name": "CheckpointStore",
                "store_backend": {
                    "class_name": "TupleFilesystemStoreBackend",
                    "suppress_store_backend_id": True,
                    "base_directory": os.path.join(ge_root_dir, "checkpoints"),
                },
            },
        },
        "expectations_store_name": "expectations_store",
        "validations_store_name": "validations_store",
        "evaluation_parameter_store_name": "evaluation_parameter_store",
        "checkpoint_store_name": "checkpoint_store",
        "data_docs_sites": {
            "local_site": {
                "class_name": "SiteBuilder",
                "show_how_to_buttons": True,
                "store_backend": {
                    "class_name": "TupleFilesystemStoreBackend",
                    "base_directory": os.path.join(ge_root_dir, "uncommitted", "data_docs", "local_site"),
                },
                "site_index_builder": {"class_name": "DefaultSiteIndexBuilder"},
            }
        },
        "anonymous_usage_statistics": {
            "data_context_id": "abcdabcd-1111-2222-3333-abcdabcdabcd",
            "enabled": False,
        },
        "notebooks": None,
        "concurrency": {"enabled": False},
    }
)
def subdag_cog_validation_task(task_id):
    with TaskGroup(**group_kwgs) as cog_validation_task_grp:
        @task()
        def validate_metadata(**kwargs):
            """
            #### Extract task
            A simple Extract task to get data ready for the rest of the data
            pipeline. In this case, getting data is simulated by reading from a
            hardcoded JSON string.
            """
            ti = kwargs["ti"]
            input_s3_uri_path = ti.xcom_pull(task_ids=task_id)
            s3_uri_path = urlparse(input_s3_uri_path)
            client = boto3.client('s3')
            results = client.list_objects(Bucket=s3_uri_path.netloc, Prefix=s3_uri_path.path.lstrip('/'))
            assert 'Contents' in results
            return "done"

        @task()
        def validate_data(**kwargs):
            """
            #### Transform task
            A simple Transform task which takes in the collection of order data and
            computes the total order value.
            {"filename": "xxx", "expectations": [{"id": "expect_column_xxx", "args": {"column": "xxxx"}}]}
            """
            ti = kwargs["ti"]
            input_s3_uri_path = ti.xcom_pull(task_ids=task_id)
            context = gx.get_context(data_context_config)
            expectation_suite_name = str(uuid.uuid4())
            context.create_expectation_suite(expectation_suite_name=expectation_suite_name)

            conf = kwargs['dag_run'].conf

            df = geotiff_to_pandas(s3_input_raster=input_s3_uri_path)
            results = []
            runtime_batch_request = RuntimeBatchRequest(
                **{
                    "datasource_name": "my_datasource",
                    "data_connector_name": "default_runtime_data_connector_name",
                    "data_asset_name": "my_alphabetical_dataframe",
                    "runtime_parameters": {"batch_data": df},
                    "batch_identifiers": {"default_identifier_name": "default_identifier"}
                }
            )
            validator = context.get_validator(
                batch_request=runtime_batch_request, expectation_suite_name=expectation_suite_name
            )
            for expectation in conf['expectations']:
                res = getattr(validator, expectation['id'])(**expectation['args'])
                results.append({
                    "expectation": res['expectation_config']['expectation_type'],
                    "kwargs": res['expectation_config']['kwargs'],
                    "status": res['success'],
                    "result": res['result']
                }
                )
            return results

        @task()
        def notify(result):
            """
            #### Load task
            A simple Load task which takes in the result of the Transform task and
            instead of saving it to end user review, just prints it out.
            """
            if not result['status']:
                raise Exception(result)
        metadata_validation = validate_metadata()
        validate_geotiff_result = validate_data(metadata_validation=metadata_validation)
        notify.expand(result=validate_geotiff_result)
        return cog_validation_task_grp