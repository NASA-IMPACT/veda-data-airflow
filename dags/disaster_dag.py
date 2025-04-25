import pendulum
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator as EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.decorators import task
from veda_data_pipeline.groups.collection_group import ingest_collection_task
from pyarc2stac.ArcReader import ArcReader
import json
from airflow.models import Variable
from veda_data_pipeline.utils.submit_stac import submission_handler
from veda_data_pipeline.utils.xcom_to_s3 import read_xcom_from_s3



dag_doc_md = """
### Collection Creation and Ingestion
Generates a collection based on the Dataset model and ingests into the catalog 
#### Notes
- This DAG can run with the following configuration <br>
```json
{
    "bucket": "bucket-name", 
    "payload": "file-to-read.json",
}
```
"""

dag_args = {
    "start_date": pendulum.today("UTC").add(days=-1),
    "schedule_interval": "0 5 * * *",
    "catchup": False,
    "doc_md": dag_doc_md,
    "tags": ["collection"],
}

template_dag_run_conf = {
    "bucket": "s3://veda-disasters", 
    "payload": "disaster_url_stacID.json", 
}


def read_url_pyarcstac(item):
    url = item["url"]
    stac_id = item["stac_id"]
    print(stac_id)

    # Retrieve data from URL
    reader = ArcReader(server_url = url)
    collection = reader.generate_stac().to_dict()
    collection['id'] = stac_id
    # reader.save_collection_to_json()

    print(collection)
    return collection




@task
def read_and_convert():
    bucket_info = dag.params
    #Read the payload from the bucket
    data = read_xcom_from_s3(f'{bucket_info.get("bucket")}/{bucket_info.get("payload")}')
    print(f'Data to process is {data}')
    collections = [read_url_pyarcstac(item) for item in data['payload']]
    return collections


with DAG("disaster-new", params=template_dag_run_conf, **dag_args) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)

    collections = read_and_convert()
    ingest_results = ingest_collection_task.expand(collection=collections)
    
    start >> collections >> ingest_results >> end



