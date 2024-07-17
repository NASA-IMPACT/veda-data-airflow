import pendulum
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
from veda_data_pipeline.groups.discover_group import subdag_discover

dag_doc_md = """
### Discover files from S3
#### Purpose
This DAG discovers files from either S3 and/or CMR then runs a DAG id `veda_ingest`. 
The DAG `veda_ingest` will run in parallel processing (2800 files per each DAG)
#### Notes
- This DAG can run with the following configuration <br>
```json
{
    "collection": "collection-id",
    "bucket": "veda-data-store-staging",
    "prefix": "s3-prefix/",
    "filename_regex": "^(.*).tif$",
    "id_regex": ".*_(.*).tif$",
    "process_from_yyyy_mm_dd": "YYYY-MM-DD",
    "id_template": "example-id-prefix-{}",
    "datetime_range": "month",
    "last_successful_execution": datetime(2015,01,01),
    "assets": {
        "asset1": {
            "title": "Asset type 1",
            "description": "First of a multi-asset item.",
            "regex": ".*asset1.*",
        },
        "asset2": {
            "title": "Asset type 2",
            "description": "Second of a multi-asset item.",
            "regex": ".*asset2.*",
        },
    }
}	
```
- [Supports linking to external content](https://github.com/NASA-IMPACT/veda-data-pipelines)
"""

dag_args = {
    "start_date": pendulum.today("UTC").add(days=-1),
    "catchup": False,
    "doc_md": dag_doc_md,
    "is_paused_upon_creation": False,
}

templat_dag_run_conf = {
    "collection": "<coll_name>",
    "bucket": "<bucket>",
    "prefix": "<prefix>/",
    "filename_regex": "<file_regex>",
    "id_regex": "<id_regex>",
    "id_template": "<id_template_string>",
    "datetime_range": "<year>|<month>|<day>",
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


def get_discover_dag(id, event={}):
    params_dag_run_conf = event or templat_dag_run_conf
    with DAG(
        id,
        schedule_interval=event.get("schedule"),
        params=params_dag_run_conf,
        **dag_args
    ) as dag:
        start = DummyOperator(task_id="Start", dag=dag)
        end = DummyOperator(
            task_id="End", trigger_rule=TriggerRule.ONE_SUCCESS, dag=dag
        )

        discover_grp = subdag_discover(event)

        start >> discover_grp >> end

        return dag


get_discover_dag("veda_discover")
