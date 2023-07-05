import pendulum
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
from veda_data_pipeline.groups.transfer_group import subdag_transfer

dag_doc_md = """
### Discover files from S3
#### Purpose
This DAG discovers files from either S3 and/or CMR then runs a DAG id `veda_ingest`. 
The DAG `veda_ingest` will run in parallel processing (2800 files per each DAG)
#### Notes
- This DAG can run with a configuration similar to this <br>
```json
{
    "origin_bucket": "covid-eo-dashboard",
    "origin_prefix": "s3-prefix/",
    "filename_regex": "^(.*).tif$",
    "target_bucket": "target_s3_bucket",
    "collection": "collection-id",
    "dry_run": true
}	
```
- [Supports linking to external content](https://github.com/NASA-IMPACT/veda-data-pipelines)
"""

dag_args = {
    "start_date": pendulum.today("UTC").add(days=-1),
    "schedule_interval": None,
    "catchup": False,
    "doc_md": dag_doc_md,
}

templat_dag_run_conf = {
    "origin_bucket": "<bucket>",
    "origin_prefix": "<prefix>/",
    "filename_regex": "<file_regex>",
    "target_bucket": "<target_bucket>",
    "collection": "<collection-id>",
    "dry_run": "true|false",
}

with DAG("veda_transfer", params=templat_dag_run_conf, **dag_args) as dag:
    start = DummyOperator(task_id="Start", dag=dag)
    end = DummyOperator(task_id="End", trigger_rule=TriggerRule.ONE_SUCCESS, dag=dag)

    transfer_grp = subdag_transfer()

    start >> transfer_grp >> end
