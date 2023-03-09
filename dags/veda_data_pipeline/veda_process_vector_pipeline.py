import pendulum
from airflow import DAG
from airflow.models.variable import Variable
from airflow.providers.amazon.aws.operators.ecs import EcsRunTaskOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
from veda_data_pipeline.groups.processing_group import subdag_process

from datetime import timedelta

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

templat_dag_run_conf = {
    "collection": "<collection_name>",
    "prefix": "<prefix>/",
    "bucket": "<bucket>",
    "filename_regex": "<filename_regex>",
    "discovery": "<s3>|cmr",
    "datetime_range": "<month>|<day>",
    "upload": "<false> | true",
    "cogify": "false | true",
    "payload": "<s3_uri_event_payload",
}
dag_args = {
    "start_date": pendulum.today("UTC").add(days=-1),
    "schedule_interval": None,
    "catchup": False,
    "doc_md": dag_doc_md,
}

with DAG(dag_id="veda_ingest_vector", params=templat_dag_run_conf, **dag_args) as dag:
    start = DummyOperator(task_id="Start", dag=dag)
    end = DummyOperator(task_id="End", trigger_rule=TriggerRule.ONE_SUCCESS, dag=dag)

    mwaa_stack_conf = Variable.get("MWAA_STACK_CONF", deserialize_json=True)

    ingest_vector = EcsRunTaskOperator(
            task_id="ingest_vector",
            trigger_rule="none_failed",
            cluster=f"{mwaa_stack_conf.get('PREFIX')}-cluster",
            task_definition=f"{mwaa_stack_conf.get('PREFIX')}-tasks",
            launch_type="FARGATE",
            do_xcom_push=True,
            execution_timeout=timedelta(minutes=60),
            overrides={
                "containerOverrides": [
                    {
                        "name": f"{mwaa_stack_conf.get('PREFIX')}-vector-ingest",
                        "command": [
                            "/usr/local/bin/python",
                            "handler.py",
                        ],
                        "environment": [
                            {
                                "name": "EXTERNAL_ROLE_ARN",
                                "value": Variable.get("ASSUME_ROLE_READ_ARN"),
                            },
                            {
                                "name": "BUCKET",
                                "value": "veda-data-pipelines-staging-lambda-ndjson-bucket",
                            },
                            {
                                "name": "EVENT_BUCKET",
                                "value": mwaa_stack_conf.get("EVENT_BUCKET"),
                            },
                        ],
                        "memory": 2048,
                        "cpu": 1024,
                    },
                ],
            },
            network_configuration={
                "awsvpcConfiguration": {
                    "securityGroups": mwaa_stack_conf.get("SECURITYGROUPS"),
                    "subnets": mwaa_stack_conf.get("SUBNETS"),
                },
            },
            awslogs_group=mwaa_stack_conf.get("LOG_GROUP_NAME"),
            awslogs_stream_prefix=f"ecs/{mwaa_stack_conf.get('PREFIX')}-vector-ingest",  # prefix with container name
        )

    start >> ingest_vector >> end
