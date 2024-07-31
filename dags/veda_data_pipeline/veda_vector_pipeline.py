import pendulum
from datetime import timedelta

from airflow import DAG
from airflow.models.variable import Variable
from airflow.providers.amazon.aws.operators.ecs import EcsRunTaskOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.models.variable import Variable

from veda_data_pipeline.groups.processing_tasks import build_vector_kwargs
from veda_data_pipeline.groups.discover_group import discover_from_s3_task, get_files_to_process


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

with DAG(dag_id="veda_ingest_vector", params=template_dag_run_conf, **dag_args) as dag:
    # ECS dependency variable
    mwaa_stack_conf = Variable.get("MWAA_STACK_CONF", deserialize_json=True)

    start = DummyOperator(task_id="Start", dag=dag)
    end = DummyOperator(task_id="End", trigger_rule=TriggerRule.ONE_SUCCESS, dag=dag)

    discover = discover_from_s3_task()
    get_files = get_files_to_process(payload=discover)
    build_vector_kwargs_task = build_vector_kwargs(event=get_files)
    vector_ingest = EcsRunTaskOperator.partial(
            task_id="ingest_vector"
            execution_timeout=timedelta(minutes=60),
            trigger_rule=TriggerRule.NONE_FAILED,
            cluster=f"{mwaa_stack_conf.get('PREFIX')}-cluster",
            task_definition=f"{mwaa_stack_conf.get('PREFIX')}-tasks",
            launch_type="FARGATE",
            do_xcom_push=True
        ).expand_kwargs(build_vector_kwargs_task)
    
    discover.set_upstream(start)
    vector_ingest.set_downstream(end)


