import logging
import pendulum
from airflow.models.param import Param
from airflow.decorators import task
from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.sdk import Variable
from slack_notifications import slack_fail_alert
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
    "extra_flags": ["-overwrite", "-lco", "OVERWRITE=YES", "-oo", "X_POSSIBLE_NAMES=latitude", "-oo", "Y_POSSIBLE_NAMES=longitude"]
    "discovered": 33,
    "payload": "s3://data-pipeline-ghgc-dev-mwaa-597746869805/events/test_layer_name2/s3_discover_output_f88257e8-ee50-4a14-ace4-5612ae6ebf38.jsonn"
    "invalidate_cloudfront": true

}
```

#### Table configuration
`table_config` applies index and statistics DDL **once, after every discovered file has
been ingested**. Omit the key entirely to skip it.

```json
{
    "table_config": {
        "schema": "public",
        "indexes": [
            {"columns": ["datetime"], "method": "btree", "concurrently": true}
        ],
        "analyze": true
    }
}
```

- Requires an explicit `collection`; with a per-file `id_template` there is no single
  table to configure, and the step is skipped.
- Indexes are built after load on purpose -- one bulk sort rather than per-row
  maintenance during ingest.
- `concurrently` defaults to true so the build does not take an `ACCESS EXCLUSIVE` lock
  on a table the Features API is serving.
- `-overwrite` drops and recreates the table, so any index is destroyed on each ingest
  and rebuilt by this step.
- **Backfilling across several DAG runs:** omit `table_config` from the intermediate runs
  and set it only on the last, otherwise the index exists while later chunks are still
  loading -- which is exactly what the post-ingest ordering avoids.

- [Supports linking to external content](https://github.com/NASA-IMPACT/veda-data-pipelines)
"""

template_dag_run_conf = {
    "collection": Param("collection_name", type="string"),
    "prefix": Param("<prefix>/", type="string",  pattern="^[^/].*/$", description="Must have a trailing slash"),
    "bucket": "<bucket>",
    "filename_regex": "<filename_regex>",
    "id_template": "<id_template_prefix>-{}",
    "datetime_range": Param(type="string", enum=["month", "day", ""], description="<month|day>", default=""),
    "vector": Param(True, type="boolean"),
    "x_possible": "<x_column_name>",
    "y_possible": "<y_column_name>",
    "source_projection": "<crs>",
    "target_projection": "<crs>",
    "extra_flags": "<args>",
    "payload": "<s3_uri_event_payload>",
    "table_config": Param(
        None,
        type=["null", "object"],
        description=(
            "Optional post-ingest table configuration, applied once after every file has "
            "been ingested. Omit it entirely to skip. Example: "
            '{"indexes": [{"columns": ["datetime"]}], "analyze": true}'
        ),
    ),
    "invalidate_cloudfront": Param(True, type="boolean")
}
dag_args = {
    "start_date": pendulum.today("UTC").add(days=-1),
    "catchup": False,
    "on_failure_callback": slack_fail_alert,
    "doc_md": dag_doc_md,
}


@task
def ingest_vector_task(payload):
    from veda_data_pipeline.utils.vector_ingest.handler import handler

    read_role_arn = Variable.get("ASSUME_ROLE_READ_ARN")
    vector_secret_name = Variable.get("VECTOR_SECRET_NAME")
    return handler(payload_src=payload, vector_secret_name=vector_secret_name,
                   assume_role_arn=read_role_arn)


@task
def configure_table(dag_run=None):
    """Apply post-ingest table configuration once every mapped ingest task has finished.

    Placed downstream of `ingest_vector_task.expand(...)` on purpose: Airflow runs a plain
    task after *all* mapped instances complete, so indexes are built once on the finished
    table rather than once per chunk. See utils/vector_ingest/table_config.py.
    """
    from veda_data_pipeline.utils.vector_ingest.table_config import apply_table_config

    conf = dag_run.conf
    table_config = conf.get("table_config")
    if not table_config:
        logging.info("No table_config provided, skipping table configuration")
        return {"status": "skipped"}

    collection = conf.get("collection")
    if not collection:
        # Without an explicit collection the ingest names a table per file from
        # id_template, so there is no single table to configure.
        logging.warning(
            "table_config requires an explicit `collection`; skipping table configuration"
        )
        return {"status": "skipped"}

    vector_secret_name = Variable.get("VECTOR_SECRET_NAME")
    return apply_table_config(collection, table_config, vector_secret_name)


@task
def invalidate_cloudfront(dag_run=None):

    if not dag_run.conf.get('invalidate_cloudfront'):
        logging.info("Skipping cloudfront invalidation")
        return

    import boto3
    try:
        cloudfront_to_invalidate_id = Variable.get("CLOUDFRONT_TO_INVALIDATE", default=None)
        cloudfront_path_to_invalidate = Variable.get("CLOUDFRONT_PATH_TO_INVALIDATE", default=None)

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
            schedule=event.get("schedule", None),
            params=template_dag_run_conf,
            **dag_args
    ) as dag:
        start = EmptyOperator(task_id="Start", dag=dag)
        end = EmptyOperator(task_id="End", trigger_rule=TriggerRule.ONE_SUCCESS, dag=dag)
        discover = start >> discover_from_s3_task(event=event)
        get_files = get_files_task(payload=discover)
        ingest_vector_task.expand(payload=get_files) >> configure_table() >> invalidate_cloudfront() >> end

        return dag


# Sending empty event because we rely on task instance (ti) for manual runs
# and payload for scheduled runs
# get_ingest_vector_dag(id="veda_ingest_vector", event={})
