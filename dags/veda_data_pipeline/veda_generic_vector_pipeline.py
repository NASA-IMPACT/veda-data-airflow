import pendulum
from datetime import timedelta

from airflow import DAG
from airflow.models.variable import Variable
from airflow.providers.amazon.aws.operators.ecs import EcsRunTaskOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.models.variable import Variable

from veda_data_pipeline.groups.processing_tasks import build_generic_vector_kwargs
from veda_data_pipeline.groups.discover_group import discover_from_s3_task, get_files_to_process


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

with DAG(dag_id="veda_generic_ingest_vector", params=template_dag_run_conf, **dag_args) as dag:
    # ECS dependency variable
    mwaa_stack_conf = Variable.get("MWAA_STACK_CONF", deserialize_json=True)

    start = DummyOperator(task_id="Start", dag=dag)
    end = DummyOperator(task_id="End", trigger_rule=TriggerRule.ONE_SUCCESS, dag=dag)

    discover = discover_from_s3_task()
    get_files = get_files_to_process(payload=discover)
    build_generic_vector_kwargs_task = build_generic_vector_kwargs.expand(event=get_files)
    vector_ingest = EcsRunTaskOperator.partial(
            task_id="generic_ingest_vector",
            execution_timeout=timedelta(minutes=60),
            trigger_rule=TriggerRule.NONE_FAILED,
            cluster=f"{mwaa_stack_conf.get('PREFIX')}-cluster",
            task_definition=f"{mwaa_stack_conf.get('PREFIX')}-generic_vector-tasks",
            launch_type="FARGATE",
            do_xcom_push=True
        ).expand_kwargs(build_generic_vector_kwargs_task)
    
    discover.set_upstream(start)
    vector_ingest.set_downstream(end)


