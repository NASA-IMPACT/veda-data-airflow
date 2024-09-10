import pendulum
from airflow import DAG
from airflow.models.variable import Variable
from airflow.decorators import task
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.trigger_rule import TriggerRule

from datetime import timedelta

dag_doc_md = """
### Generic Ingest Vector
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

templat_dag_run_conf = {
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

with DAG(dag_id="veda_generic_ingest_vector", params=templat_dag_run_conf, **dag_args) as dag:
    start = DummyOperator(task_id="Start", dag=dag)
    end = DummyOperator(task_id="End", trigger_rule=TriggerRule.ONE_SUCCESS, dag=dag)
    @task()
    def generic_ingest_vector(ti):
        from veda_data_pipeline.utils.vector_ingest import handler
        conf = ti.dag_run.conf.copy()
        secret_name = Variable.get("VECTOR_SECRET_NAME")
        return handler(secret_name, conf)


    start >> generic_ingest_vector() >> end
