import pendulum
from airflow.decorators import task
from airflow import DAG
from airflow.models.variable import Variable
import json
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
from veda_data_pipeline.groups.discover_group import discover_from_s3_task, get_files_to_process

dag_doc_md = """
### Generic Ingest Vector
#### Purpose
This DAG is used to ingest vector data for use in the VEDA Features API

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
    "vector": true,
    "id_regex": "",
    "id_template": "NIST_Urban_Testbed_test-{}",
    "datetime_range": "",
    "vector": true,
    "x_possible": "longitude",
    "y_possible": "latitude",
    "source_projection": "EPSG:4326",
    "target_projection": "EPSG:4326",
    "extra_flags": ["-overwrite", "-lco", "OVERWRITE=YES"]
    "discovered": 33,
    "payload": "s3://data-pipeline-ghgc-dev-mwaa-597746869805/events/test_layer_name2/s3_discover_output_f88257e8-ee50-4a14-ace4-5612ae6ebf38.jsonn"
}	
```
- [Supports linking to external content](https://github.com/NASA-IMPACT/veda-data-pipelines)
"""

template_dag_run_conf = {
    "collection": "<collection_name>",
    "prefix": "<prefix>/",
    "bucket": "<bucket>",
    "filename_regex": "<filename_regex>",
    "id_template": "<id_template_prefix>-{}",
    "datetime_range": "<month>|<day>",
    "vector": "false | true",
    "x_possible": "<x_column_name>",
    "y_possible": "<y_column_name>",
    "source_projection": "<crs>",
    "target_projection": "<crs>",
    "extra_flags": "<args>",
    "payload": "<s3_uri_event_payload>",
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


with DAG(dag_id="veda_generic_ingest_vector", params=template_dag_run_conf, **dag_args) as dag:

    start = DummyOperator(task_id="Start", dag=dag)
    end = DummyOperator(task_id="End", trigger_rule=TriggerRule.ONE_SUCCESS, dag=dag)
    discover = discover_from_s3_task()
    get_files = get_files_to_process(payload=discover)
    vector_ingest = ingest_vector_task.expand(payload=get_files)
    discover.set_upstream(start)
    vector_ingest.set_downstream(end)
