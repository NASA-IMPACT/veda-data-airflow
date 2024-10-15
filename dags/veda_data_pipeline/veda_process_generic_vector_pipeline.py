from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.models.variable import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.amazon.aws.operators.ecs import EcsRunTaskOperator
from airflow.utils.trigger_rule import TriggerRule

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

    mwaa_stack_conf = Variable.get(
        "MWAA_STACK_CONF", default_var={}, deserialize_json=True
    )
    vector_ecs_conf = Variable.get("VECTOR_ECS_CONF",default_var={}, deserialize_json=True)

    generic_ingest_vector = EcsRunTaskOperator(
        task_id="generic_ingest_vector",
        trigger_rule=TriggerRule.NONE_FAILED,
        cluster=f"{mwaa_stack_conf.get('PREFIX')}-cluster",
        task_definition=f"{mwaa_stack_conf.get('PREFIX')}-generic-vector-tasks",
        launch_type="FARGATE",
        do_xcom_push=True,
        execution_timeout=timedelta(minutes=120),
        overrides={
            "containerOverrides": [
                {
                    "name": f"{mwaa_stack_conf.get('PREFIX')}-veda-generic_vector_ingest",
                    "command": [
                        "/var/lang/bin/python",
                        "handler.py",
                        "--payload",
                        "{}".format("{{ task_instance.dag_run.conf }}"),
                    ],
                    "environment": [
                        {
                            "name": "EXTERNAL_ROLE_ARN",
                            "value": Variable.get(
                                "ASSUME_ROLE_READ_ARN", default_var=""
                            ),
                        },
                        {
                            "name": "AWS_REGION",
                            "value": mwaa_stack_conf.get("AWS_REGION"),
                        },
                        {
                            "name": "VECTOR_SECRET_NAME",
                            "value": Variable.get("VECTOR_SECRET_NAME", default_var=""),
                        },
                    ],
                },
            ],
        },
        network_configuration={
            "awsvpcConfiguration": {
                    "securityGroups": vector_ecs_conf.get("VECTOR_SECURITY_GROUP", "") +
                                      mwaa_stack_conf.get("SECURITYGROUPS", ""),
                    "subnets": vector_ecs_conf.get("VECTOR_SUBNETS"),
            },
        },
        awslogs_group=mwaa_stack_conf.get("LOG_GROUP_NAME"),
        awslogs_stream_prefix=f"ecs/{mwaa_stack_conf.get('PREFIX')}-veda-generic-vector_ingest",  # prefix with container name
    )

    start >> generic_ingest_vector >> end
