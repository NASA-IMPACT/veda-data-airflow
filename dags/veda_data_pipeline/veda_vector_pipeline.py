import pendulum
from airflow.decorators import task
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.models.variable import Variable
from veda_data_pipeline.groups.discover_group import discover_from_s3_task, get_files_to_process
import json

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

template_dag_run_conf = {
    "collection": "<collection_name>",
    "prefix": "<prefix>/",
    "bucket": "<bucket>",
    "filename_regex": "<filename_regex>",
    "discovery": "<s3>|cmr",
    "datetime_range": "<month>|<day>",
    "upload": "<false> | true",
    "cogify": "false | true"
}
dag_args = {
    "start_date": pendulum.today("UTC").add(days=-1),
    "schedule_interval": None,
    "catchup": False,
    "doc_md": dag_doc_md,
}


@task
def ingest_vector_task(payload):
    from veda_data_pipeline.utils.vector_ingest.handler import handler

    airflow_vars = Variable.get("aws_dags_variables")
    airflow_vars_json = json.loads(airflow_vars)
    read_role_arn = airflow_vars_json.get("ASSUME_ROLE_READ_ARN")
    vector_secret_name = airflow_vars_json.get("VECTOR_SECRET_NAME")
    return handler(payload_src=payload, vector_secret_name=vector_secret_name,
                   assume_role_arn=read_role_arn)


with DAG(dag_id="veda_ingest_vector", params=template_dag_run_conf, **dag_args) as dag:
    start = DummyOperator(task_id="Start", dag=dag)
    end = DummyOperator(task_id="End", trigger_rule=TriggerRule.ONE_SUCCESS, dag=dag)
    discover = discover_from_s3_task()
    get_files = get_files_to_process(payload=discover)
    vector_ingest = ingest_vector_task.expand(payload=get_files)
    discover.set_upstream(start)
    vector_ingest.set_downstream(end)
