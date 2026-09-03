import pendulum
from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.models.param import Param
from veda_data_pipeline.groups.discover_group import discover_from_s3_task, get_files_task
from veda_data_pipeline.groups.disasters_group import enrich_disaster_metadata, EXTRACTORS
from slack_notifications import slack_fail_alert

from veda_data_pipeline.groups.processing_tasks import submit_to_stac_ingestor_task, build_stac_task

dag_doc_md = f"""
### Discover disasters files from S3
#### Purpose
Same as `veda_discover`, with an added post-discovery step that annotates each
discovered item with disasters metadata read from the item's GeoTIFF tags (or from
its S3 path) before the items are built into STAC.

Use this DAG instead of `veda_discover` when a collection needs that metadata; the
enrichment step opens every discovered file, so it is not free.

#### Extractors
`disasters_extractors` selects which metadata is added:

- `monty` - event name, `monty:country_codes`, `monty:hazard_codes`,
  `monty:corr_id`, plus `hazard`/`location` arrays. Superset of `event_name`.
- `event_name` - `event:name` only, read from the GeoTIFF `EVENT` tag.
- `product` - `sensor` and `product`, from the last two path segments.
- `providers` - `providers` list, from the GeoTIFF `PROVIDERS` tag.

An extractor that cannot find a value contributes nothing rather than a null
property. Passing an unrecognised name fails the task.

#### Notes
- This DAG can run with the following configuration <br>
```json
{{
    "collection": "collection-id",
    "bucket": "veda-data-store-staging",
    "prefix": "s3-prefix/",
    "filename_regex": "^(.*).tif$",
    "id_regex": ".*_(.*).tif$",
    "id_template": "example-id-prefix-{{}}",
    "datetime_range": "month",
    "disasters_extractors": {sorted(EXTRACTORS)},
    "assets": {{
        "asset1": {{
            "title": "Asset type 1",
            "description": "First of a multi-asset item.",
            "regex": ".*asset1.*",
        }},
        "asset2": {{
            "title": "Asset type 2",
            "description": "Second of a multi-asset item.",
            "regex": ".*asset2.*",
        }},
    }}
}}
```
"""

dag_args = {
    "start_date": pendulum.today("UTC").add(days=-1),
    "catchup": False,
    "doc_md": dag_doc_md,
    "on_failure_callback": slack_fail_alert,
    "is_paused_upon_creation": False,
    "tags": ["disasters", "discovery"],
}

template_dag_run_conf = {
    "collection": Param("collection_name", type="string"),
    "bucket": "<bucket>",
    "prefix": "<prefix>/",
    "filename_regex": "<file_regex>",
    "id_regex": "<id_regex>",
    "id_template": "<id_template_string>",
    "datetime_range": Param(type="string", enum=["year", "month", "day", ""], description="<year|month|day>", default=""),
    "disasters_extractors": Param(
        ["monty"],
        type="array",
        items={"type": "string"},
        description=f"Metadata extractors to apply. One or more of: {sorted(EXTRACTORS)}",
    ),
    "assets": {
        "<asset1_name>": {
            "title": "<asset_title>",
            "description": "<asset_description>",
            "regex": "<asset_regex>",
        },
        "<asset2_name>": {
            "title": "<asset_title>",
            "description": "<asset_description>",
            "regex": "<asset_regex>",
        },
    },
}


def get_disasters_discover_dag(id: str, event: dict):

    with DAG(
            id,
            schedule=event.get("schedule"),
            params=template_dag_run_conf,
            **dag_args
    ) as dag:
        start = EmptyOperator(task_id="Start", dag=dag)
        end = EmptyOperator(
            task_id="End", dag=dag
        )
        # define DAG using taskflow notation

        discover = discover_from_s3_task(event=event)
        # Passing None lets the task fall back to the DAG run config, so manual
        # runs can choose extractors without a scheduled event.
        enrich = enrich_disaster_metadata(
            discovery_result=discover,
            extractors=event.get("disasters_extractors"),
        )
        get_files = get_files_task(payload=enrich)
        build_stac = build_stac_task.expand(payload=get_files)
        # .output is needed coming from a non-taskflow operator
        submit_stac = submit_to_stac_ingestor_task.expand(built_stac=build_stac)

        start >> discover
        submit_stac >> end

        return dag
