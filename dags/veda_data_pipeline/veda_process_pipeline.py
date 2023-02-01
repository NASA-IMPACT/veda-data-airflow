from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
import pendulum

from veda_data_pipeline.groups.processing_group import subdag_process
dag_doc_md = """
### Build and submit stac
#### Purpose
This DAG is supposed to be triggered by `veda_discover`. But you still can trigger this DAG manually or through an API 

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
    "cogify": false,
    "discovered": 33,
    "payload": "s3://veda-uah-sit-mwaa-853558080719/events/geoglam/s3_discover_output_6c46b57a-7474-41fe-977a-19d164531cdc.json"
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

with DAG("veda_ingest", **dag_args) as dag:
    start = DummyOperator(task_id="Start", dag=dag)
    end = DummyOperator(task_id="End", trigger_rule=TriggerRule.ONE_SUCCESS, dag=dag)

    process_grp = subdag_process()

    start >> process_grp >> end
