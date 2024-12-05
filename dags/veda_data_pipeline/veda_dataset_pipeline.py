import pendulum
from airflow import DAG
from airflow.models.param import Param
from airflow.decorators import task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.dummy_operator import DummyOperator as EmptyOperator
from veda_data_pipeline.groups.collection_group import collection_task_group

template_dag_run_conf = {
    "collection": "<collection-id>",
    "data_type": "cog",
    "description": "<collection-description>",
    "discovery_items": [
        {
            "bucket": "<bucket-name>",
            "datetime_range": "<range>",
            "discovery": "s3",
            "filename_regex": "<regex>",
            "prefix": "<example-prefix/>",
        }
    ],
    "is_periodic": "<true|false>",
    "license": "<collection-LICENSE>",
    "time_density": "<time-density>",
    "title": "<collection-title>",
}

dag_doc_md = f"""
### Dataset Pipeline
Generates a collection and triggers the file discovery process
#### Notes
- This DAG can run with the following configuration <br>
```json
{template_dag_run_conf}
```
"""

dag_args = {
    "start_date": pendulum.today("UTC").add(days=-1),
    "schedule_interval": None,
    "catchup": False,
    "doc_md": dag_doc_md,
    "tags": ["collection", "discovery"],
}

<<<<<<< HEAD

@task
def extract_discovery_items(**kwargs):
    ti = kwargs.get("ti")
    discovery_items = ti.dag_run.conf.get("discovery_items")
    print(discovery_items)
    return discovery_items


@task(max_active_tis_per_dag=3)
def build_stac_task(payload):
    from veda_data_pipeline.utils.build_stac.handler import stac_handler
    airflow_vars = Variable.get("aws_dags_variables")
    airflow_vars_json = json.loads(airflow_vars)
    event_bucket = airflow_vars_json.get("EVENT_BUCKET")
    return stac_handler(payload_src=payload, bucket_output=event_bucket)

@task()
def mutate_payload(**kwargs):
    ti = kwargs.get("ti")
    payload = ti.dag_run.conf
    if assets := payload.get("assets"):
        # remove thumbnail asset if provided in collection config
        if "thumbnail" in assets.keys():
            assets.pop("thumbnail")
        # if thumbnail was only asset, delete assets
        if not assets:
            payload.pop("assets")
        # finally put the mutated assets back in the payload
        else:
            payload["assets"] = assets
    return payload


template_dag_run_conf = {
    "collection": "<collection-id>",
    "data_type": "cog",
    "description": "<collection-description>",
    "discovery_items":
        [
            {
                "bucket": "<bucket-name>",
                "datetime_range": "<range>",
                "discovery": "s3",
                "filename_regex": "<regex>",
                "prefix": "<example-prefix/>"
            }
        ],
    "is_periodic": "<true|false>",
    "license": "<collection-LICENSE>",
    "time_density": "<time-density>",
    "title": "<collection-title>"
}

=======
>>>>>>> 948aaecda0ca0d342410a2c7b8c1585d6072056e
with DAG("veda_dataset_pipeline", params=template_dag_run_conf, **dag_args) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")


<<<<<<< HEAD
    collection_grp = collection_task_group()
    mutate_payload_task = mutate_payload()
    discover = discover_from_s3_task.partial(alt_payload=mutate_payload_task).expand(event=extract_discovery_items())
    discover.set_upstream(collection_grp)  # do not discover until collection exists
    get_files = get_dataset_files_to_process(payload=discover)
=======
    @task
    def get_items(**kwargs):
        ti = kwargs['ti']
        return ti.dag_run.conf.get('discovery_items')
>>>>>>> 948aaecda0ca0d342410a2c7b8c1585d6072056e


<<<<<<< HEAD
    collection_grp.set_upstream(start)
    mutate_payload_task.set_upstream(start)
    submit_stac.set_downstream(end)
=======
    items = start >> collection_task_group() >> get_items()
    run_discover_build_and_push = TriggerDagRunOperator.partial(
        task_id="trigger_discover_items_dag",
        trigger_dag_id="veda_discover",
        wait_for_completion=True,

    ).expand(conf=items) >> end
>>>>>>> 948aaecda0ca0d342410a2c7b8c1585d6072056e
