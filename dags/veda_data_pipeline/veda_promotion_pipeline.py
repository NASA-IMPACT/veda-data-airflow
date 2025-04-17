import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.models.variable import Variable
from airflow.models.param import Param

import json
from veda_data_pipeline.groups.collection_group import collection_task_group
from veda_data_pipeline.groups.discover_group import discover_from_s3_task, get_dataset_files_to_process
from veda_data_pipeline.groups.processing_tasks import submit_to_stac_ingestor_task, build_stac_task, extract_discovery_items_from_payload, remove_thumbnail_asset
from veda_data_pipeline.groups.transfer_group import transfer_data

dag_doc_md = """
### Promotion Pipeline
Generates a collection and triggers the file discovery process.
This DAG uses the same input ad `veda-dataset-pipeline` but adds the ability to transfer assets to the production bucket.
This will mutate the payload, so that item references will target the new asset locations.
#### Notes
- This DAG can run with the following configuration <br>
```json
{
    "collection": "collection-id",
    "data_type": "cog",
    "description": "collection description",
    "discovery_items":
        [
            {
                "bucket": "veda-data-store-staging",
                "datetime_range": "year",
                "discovery": "s3",
                "filename_regex": "^(.*).tif$",
                "prefix": "example-prefix/"
            }
        ],
    "is_periodic": true,
    "license": "collection-LICENSE",
    "time_density": "year",
    "title": "collection-title",
    "transfer": "false"
}
```
"""

dag_args = {
    "start_date": pendulum.today("UTC").add(days=-1),
    "schedule": None,
    "catchup": False,
    "doc_md": dag_doc_md,
    "tags": ["collection", "discovery"],
}

template_dag_run_conf = {
    "collection": "<collection-id>",
    "data_type": "cog",
    "description": "<collection-description>",
    "discovery_items": [
        {
            "bucket": "<bucket-name>",
            "datetime_range": "<range>",
            "discovery": "s3",
            "filename_regex": "<regex>",
            "prefix": "<example-prefix/>"
        }
    ],
    "is_periodic": "<true|false>",
    "license": "<collection-LICENSE>",
    "time_density": "<time-density>",
    "title": "<collection-title>",
    "transfer": Param(True, type="boolean", description="Transfer assets to production bucket if true (true by default)"),
}

@task(max_active_tis_per_dag=3)
def transfer_assets_to_production_bucket(ti=None, payload={}):
    # merge collection id into payload, then transfer data
    payload["collection"] = ti.dag_run.conf.get("collection")
    transfer = payload.get("transfer", ti.dag_run.conf.get("transfer", True))

    config = {
        **payload,
        "origin_bucket": payload.get("bucket", ti.dag_run.conf.get("origin_bucket", "veda-data-store")),
        "origin_prefix": payload.get("prefix", ti.dag_run.conf.get("origin_prefix", "s3-prefix/")),
        "target_bucket": payload.get("target_bucket", ti.dag_run.conf.get("target_bucket", "veda-data-store")),
        "dry_run": payload.get("dry_run", ti.dag_run.conf.get("dry_run", False)),
    }

    if not transfer:
      print(f"Transfer is disabled. Skipping transfer.")
      return payload
    else:
        transfer_data(payload=config)

        # if transfer complete, update discovery payload to reflect new bucket
        payload.update({"bucket": "veda-data-store"})
        payload.update({"prefix": payload.get("collection")+"/"})
        return payload

with DAG("veda_promotion_pipeline", params=template_dag_run_conf, **dag_args) as dag:
    # ECS dependency variable

    start = EmptyOperator(task_id="start", dag=dag)
    end = EmptyOperator(task_id="end", dag=dag)

    collection_grp = collection_task_group()
    mutate_payload_task = remove_thumbnail_asset()
    extract_from_payload = extract_discovery_items_from_payload()

    # asset transfer to production bucket
    transfer_task = transfer_assets_to_production_bucket.expand(payload=extract_from_payload)
    discover = discover_from_s3_task.partial(payload=mutate_payload_task).expand(event=transfer_task)
    collection_grp >> discover  # do not discover until collection exists

    get_files = get_dataset_files_to_process(payload=discover) # untangle mapped data format to get iterable payloads from discover step
    build_stac = build_stac_task.expand(payload=get_files)
    submit_stac = submit_to_stac_ingestor_task.expand(built_stac=build_stac)

    start >> [collection_grp, mutate_payload_task, extract_from_payload]
    submit_stac >> end
