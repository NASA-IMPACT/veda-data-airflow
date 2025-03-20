import logging
import pendulum
from airflow.models.param import Param
from airflow.decorators import task
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.models.variable import Variable
from veda_data_pipeline.groups.discover_group import discover_from_s3_task, get_files_task

dag_doc_md = """
### Build and submit stac
#### Purpose
This DAG is supposed to be triggered by `veda_discover`. But you still can trigger this DAG manually or through an API 

#### Notes
- This DAG can run with the following configuration <br>
```json
{
    "collection": "",
    "prefix": "transformed_csv/",
    "bucket": "ghgc-data-store-develop",
    "filename_regex": ".*.csv$",
    "discovery": "s3",
    "datetime_range": "month",
    "id_regex": "",
    "id_template": "NIST_Urban_Testbed_test-{}",
    "datetime_range": "",
    "vector": true,
    "source_projection": "EPSG:4326",
    "target_projection": "EPSG:4326",
    "extra_flags": ["-overwrite", "-lco", "OVERWRITE=YES", "-oo", "X_POSSIBLE_NAMES=latitude", "-oo", "Y_POSSIBLE_NAMES=lomgitude"]
    "discovered": 33,
    "payload": "s3://data-pipeline-ghgc-dev-mwaa-597746869805/events/test_layer_name2/s3_discover_output_f88257e8-ee50-4a14-ace4-5612ae6ebf38.jsonn"
    "invalidate_cloudfront": true

}	
```
- [Supports linking to external content](https://github.com/NASA-IMPACT/veda-data-pipelines)
"""

template_dag_run_conf = {
    "collection": Param("collection_name", type="string"),
    "prefix": Param("<prefix>/", type="string",  pattern="^[^/].*/$", description="Must have a trailing slash"),
    "bucket": "<bucket>",
    "filename_regex": "<filename_regex>",
    "id_template": "<id_template_prefix>-{}",
    "datetime_range": Param("month", type="string", enum=["month", "day"], description="<month|day>"),
    "vector": Param(True, type="boolean"),
    "x_possible": "<x_column_name>",
    "y_possible": "<y_column_name>",
    "source_projection": "<crs>",
    "target_projection": "<crs>",
    "extra_flags": "<args>",
    "payload": "<s3_uri_event_payload>",
    "invalidate_cloudfront": Param(True, type="boolean")
}
dag_args = {
    "start_date": pendulum.today("UTC").add(days=-1),
    "catchup": False,
    "doc_md": dag_doc_md,
}


@task
def ingest_vector_task(payload):
    from veda_data_pipeline.utils.vector_ingest.handler import handler

    airflow_vars_json = Variable.get("aws_dags_variables", deserialize_json=True)
    read_role_arn = airflow_vars_json.get("ASSUME_ROLE_READ_ARN")
    vector_secret_name = airflow_vars_json.get("VECTOR_SECRET_NAME")
    return handler(payload_src=payload, vector_secret_name=vector_secret_name,
                   assume_role_arn=read_role_arn)


@task
def invalidate_cloudfront(event: dict={}):
    import boto3
    if not event.get("invalidate_cloudfront", False):
        logging.info("Skipping cloudfront invalidation")
        return

    try:
        airflow_vars_json = Variable.get("aws_dags_variables", deserialize_json=True)
        cloudfront_to_invalidate_id = airflow_vars_json.get("CLOUDFRONT_TO_INVALIDATE")
        cloudfront_path_to_invalidate = airflow_vars_json.get("CLOUDFRONT_PATH_TO_INVALIDATE")

        if cloudfront_to_invalidate_id and cloudfront_path_to_invalidate:
            client = boto3.client('cloudfront')
            response = client.create_invalidation(
                DistributionId=cloudfront_to_invalidate_id,
                InvalidationBatch={
                    'Paths': {
                        'Quantity': 1,
                        'Items': [cloudfront_path_to_invalidate]
                    },
                    'CallerReference': str(hash(cloudfront_path_to_invalidate))
                }
            )

            print(f"Invalidation created: {response['Invalidation']['Id']}")
            return response

        logging.error("Missing CloudFront distribution ID or path to invalidate.")
    except Exception as e:
        logging.error(f"Error invalidating CloudFront: {e}")


def get_ingest_vector_dag(id: str, event: dict):
    with DAG(
            id,
            schedule_interval=event.get("schedule", None),
            params=event,
            **dag_args
    ) as dag:
        start = DummyOperator(task_id="Start", dag=dag)
        end = DummyOperator(task_id="End", trigger_rule=TriggerRule.ONE_SUCCESS, dag=dag)
        discover = start >> discover_from_s3_task(event=event)
        get_files = get_files_task(payload=discover)
        ingest_vector_task.expand(payload=get_files) >> invalidate_cloudfront(event=event) >> end

        return dag


get_ingest_vector_dag(id="veda_ingest_vector", event=template_dag_run_conf)
