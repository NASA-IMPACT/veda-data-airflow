from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
import pendulum

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
    "collection": "geoglam",
    "prefix": "geoglam/",
    "bucket": "veda-data-store-staging",
    "filename_regex": "^(.*).tif$",
    "discovery": "s3",
    "datetime_range": "month",
    "upload": false,
    "cogify": false
}	
```
- [Supports linking to external content](https://github.com/NASA-IMPACT/veda-data-pipelines)
"""
dag_args = {
    "start_date": pendulum.today("UTC").add(days=-1),
    "schedule_interval": None,
    "catchup": False,
    "doc_md": dag_doc_md
}

with DAG("veda_discover", **dag_args) as dag:
    start = DummyOperator(task_id="Start", dag=dag)
    end = DummyOperator(task_id="End", trigger_rule=TriggerRule.ONE_SUCCESS, dag=dag)

    discover_grp = subdag_discover()

    start >> discover_grp >> end
