import pendulum
from airflow import DAG
from airflow.models.variable import Variable
from airflow.providers.amazon.aws.operators.ecs import EcsRunTaskOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.trigger_rule import TriggerRule

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

    mwaa_stack_conf = Variable.get("MWAA_STACK_CONF", default_var={}, deserialize_json=True)
    vector_ecs_conf = Variable.get("VECTOR_ECS_CONF", default_var={}, deserialize_json=True)

    ingest_vector = EcsRunTaskOperator(
        task_id="ingest_vector",
        trigger_rule=TriggerRule.NONE_FAILED,
        cluster=f"{mwaa_stack_conf.get('PREFIX')}-cluster",
        task_definition=f"{mwaa_stack_conf.get('PREFIX')}-vector-tasks",
        launch_type="FARGATE",
        do_xcom_push=True,
        execution_timeout=timedelta(minutes=120),
        overrides={
            "containerOverrides": [
                {
                    "name": f"{mwaa_stack_conf.get('PREFIX')}-veda-vector_ingest",
                    "command": [
                        "/var/lang/bin/python",
                        "handler.py",
                        "--payload",
                        "{}".format("{{ task_instance.dag_run.conf }}"),
                    ],
                    "environment": [
                        {
                            "name": "EXTERNAL_ROLE_ARN",
                            "value": Variable.get("ASSUME_ROLE_READ_ARN", default_var=None),
                        },
                        {
                            "name": "AWS_REGION",
                            "value": mwaa_stack_conf.get("AWS_REGION"),
                        },
                        {
                            "name": "VECTOR_SECRET_NAME",
                            "value": Variable.get("VECTOR_SECRET_NAME"),
                        },
                    ],
                },
            ],
        },
        network_configuration={
            "awsvpcConfiguration": {
                "securityGroups": vector_ecs_conf.get("VECTOR_SECURITY_GROUP"),
                "subnets": vector_ecs_conf.get("VECTOR_SUBNETS"),
            },
        },
        awslogs_group=mwaa_stack_conf.get("LOG_GROUP_NAME"),
        awslogs_stream_prefix=f"ecs/{mwaa_stack_conf.get('PREFIX')}-veda-vector_ingest",  # prefix with container name
    )

    start >> ingest_vector >> end
