from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.datasets import Dataset
from datetime import datetime
from airflow.utils.trigger_rule import TriggerRule
from airflow.models.variable import Variable
from airflow.decorators import task

from veda_data_pipeline.utils.s3_discovery import get_s3_resp_iterator, discover_from_s3, assume_role
from veda_data_pipeline.groups.discover_group import subdag_discover, get_discover_dag
from veda_data_pipeline.utils.models import *

import boto3
import csv
import json
from io import StringIO

from collections import namedtuple


############################################################################################################


mwaa_stac_conf = Variable.get("MWAA_STACK_CONF", deserialize_json=True)
event_bucket = mwaa_stac_conf["EVENT_BUCKET"]

client = boto3.client("s3")
response = client.list_objects_v2(Bucket=event_bucket, Prefix="collections/")

tracked_events = []
for file_ in response["Contents"]:
    key = file_["Key"]
    if key.endswith("/"):
        continue
    result = client.get_object(Bucket=event_bucket, Key=key)
    collection = result["Body"].read().decode()
    collection = json.loads(collection)
    if collection.get("schedule"):
        tracked_events.append(collection)

# get this from s3 "events"
DatasetTuple = namedtuple('TrackedDataset', ['collection_id', 's3_bucket', 's3_prefix', 's3_regex', 'frequency'])

TARGET_DATA_STORE = 'veda-data-store-staging' # TODO this could be a parameter or env var

## Define your datasets (assumes that transfer to TARGET_DATA_STORE *should* happen)
#TRACKED_DATASETS = [
#    # TODO check whether covid-eo-data access requires anonymous session
#    DatasetTuple('no2-monthly', 'covid-eo-data', 'OMNO2d_HRM/', '^(.*).tif$', 'monthly'), 
#    DatasetTuple('no2-monthly-diff', 'covid-eo-data', 'OMNO2d_HRMDifference/', '^(.*).tif$', 'monthly'),
#]
#
#TRACKED_DATASET_MODELS = [
#    DiscoveryModel(collection='no2-monthly', bucket='covid-eo-data', prefix='OMNO2d_HRM/', filename_regex='^(.*).tif$', datetime_range='monthly'),
#    DiscoveryModel(collection='no2-monthly-diff', bucket='covid-eo-data', prefix='OMNO2d_HRMDifference/', filename_regex='^(.*).tif$', datetime_range='monthly')
#]

############################################################################################################


def list_incoming_s3_files_task(ti, metadata: DatasetTuple):
    read_assume_arn = Variable.get("ASSUME_ROLE_READ_ARN", default_var=None)
    s3_kwargs = assume_role(role_arn=read_assume_arn) if read_assume_arn else {}
    s3_client = boto3.client("s3", **s3_kwargs)
    s3_iterator = get_s3_resp_iterator(
        bucket_name=metadata.s3_bucket, prefix=metadata.s3_prefix, s3_client=s3_client
    )
    file_data = [
        (f"{obj['Key']}", obj['ETag'], obj['Size'])
        for obj in discover_from_s3(s3_iterator, metadata.s3_regex)
    ]

    # this update will feed into the Airflow Dataset
    # write list of files to csv in the TARGET_DATA_STORE
    csv_file_key = f"veda_ingest_metadata/{collection_id}/incoming_{metadata.collection_id}.csv"
    csv_buffer = StringIO()
    csv_writer = csv.writer(csv_buffer)
    csv_writer.writerow(["key", "etag", "size"])
    for key, etag, size in file_data:
        csv_writer.writerow([key, etag, size])

    # Upload CSV to S3
    s3_client.put_object(Bucket=TARGET_DATA_STORE, Key=csv_file_key, Body=csv_buffer.getvalue())
    
    return f"s3://{TARGET_DATA_STORE}/{csv_file_key}"

def compare_and_update_collection_assets_task(ti, collection_id:str):
    asset_inventory_key = f"veda_ingest_metadata/{collection_id}.csv"
    incoming_files_key = f"veda_ingest_metadata/incoming_{collection_id}.csv"

    read_assume_arn = Variable.get("ASSUME_ROLE_READ_ARN", default_var=None)
    s3_kwargs = assume_role(role_arn=read_assume_arn) if read_assume_arn else {}
    s3_client = boto3.client("s3", **s3_kwargs)
    
    def read_csv_from_s3(bucket, key):
        response = s3_client.get_object(Bucket=bucket, Key=key)
        csv_content = response['Body'].read().decode('utf-8')
        reader = csv.DictReader(StringIO(csv_content))
        # contains fields: key, etag, size
        return [row for row in reader]

    asset_inventory = read_csv_from_s3(TARGET_DATA_STORE, asset_inventory_key)
    incoming_files = read_csv_from_s3(TARGET_DATA_STORE, incoming_files_key)
    asset_uris = {item['key'] for item in asset_inventory}
    
    # TODO - we use keys only, no consideration of ETAG or size yet
    new_files = [file for file in incoming_files if file['key'] not in asset_uris]
    if new_files:
        pass

    return len(new_files)

def initiate_transfer_and_dicovery_task(ti, collection_id:str, frequency:str):
    # TODO it would be ideal for this to exclusively transfer new data, but this would require a refactor of our transfer DAG, and is out of scope for this PR

    # build event for transfer
    payload = {
        "collection": collection_id,
        "bucket": TARGET_DATA_STORE,
        "prefix": f"", # TODO fix
        "filename_regex": "", # TODO fix
        "datetime_range": frequency
    }

    pass

############################################################################################################



for metadata in tracked_events:
    collection_id = metadata.collection_id
    schedule_interval = metadata.get('schedule')
    tracked_dataset = Dataset(f"{collection_id}")
    # TODO these dataset IDs can be abbreviated for UI readability
    # URIs can be interpreted later on, the important one is the discovery_event for each collection as that triggers a new DAG on update
    incoming_files = f"s3://{TARGET_DATA_STORE}/veda_ingest_metadata/{collection_id}/incoming_{collection_id}_incoming.csv"
    asset_inventory = f"s3://{TARGET_DATA_STORE}/veda_ingest_metadata/{collection_id}/{collection_id}_inventory.csv"
    discovery_event = f"s3://{TARGET_DATA_STORE}/veda_ingest_metadata/{collection_id}_event.json"
    # not used yet, but included as an example
    # could store additional properties to associate with items in the collection (ie render, assets, etc.)
    template_file = f"s3://{TARGET_DATA_STORE}/veda_ingest_metadata/{collection_id}_template.json"


    with DAG(dag_id=f'veda_automated_ingest_{collection_id}',
            start_date=datetime(2024, 1, 1),
            schedule_interval='@monthly',
            tags=['automated'],  # use this to be able to filter out automated DAGs in UI
            catchup=False) as dag:
        
        start = DummyOperator(task_id='start', dag=dag)
        end = DummyOperator(task_id='end', trigger_rule=TriggerRule.ONE_SUCCESS, dag=dag)
        
        list_incoming_s3_files = PythonOperator(
            task_id='list_incoming_s3_files',
            python_callable=list_incoming_s3_files_task
        )
        
        compare_and_update_collection_assets = PythonOperator(
            task_id='compare_and_update_collection_assets',
            python_callable=compare_and_update_collection_assets_task,
            op_kwargs={'collection_id': collection_id}
        )

        # end if no new files are available
        @task.branch(task_id="branch_on_files_available")
        def branch_func(ti=None):
            xcom_value = ti.xcom_pull(task_ids="compare_and_update_collection_assets")
            if xcom_value >= 1:
                return "continue_task"
            else:
                return "end"
            
        branch_op = branch_func()
        
        initiate_transfer_and_dicovery = PythonOperator( # generate payload and feed into secondary DAG
            task_id='initiate_transfer_and_dicovery',
            python_callable=initiate_transfer_and_dicovery_task,
            op_kwargs={'collection_id': collection_id}, 
            outlets=[tracked_dataset]
        )

        start >> list_incoming_s3_files >> compare_and_update_collection_assets >> branch_op >> [ initiate_transfer_and_dicovery >> end , end ]
        # end state should mark dataset as updated


    processing_dag_template = {
        "collection": "<coll_name>",
        "bucket": "<bucket>",
        "prefix": "<prefix>/",
        "filename_regex": "<file_regex>",
        "id_regex": "<id_regex>",
        "id_template": "<id_template_string>",
        "datetime_range": "<year>|<month>|<day>",
        "assets": {
            "<asset1_name>": {
                "title": "<asset_title>",
                "description": "<asset_description>",
                "regex": "<asset_regex>",
            },
            "<asset2_name>": {
                "title": "<asset_title>",
                "description": "<asset_description>",
                "regex": "<asset_regex>",
            },
        },
    }
    with DAG(dag_id='veda_discovery_{collection_id}',
                params=processing_dag_template,
                tags=['automated'],
                start_date=datetime(2024, 1, 1),
                schedule=[tracked_dataset]) as dag:
        
        # 
        
        start = DummyOperator(task_id='start', dag=dag)
        end = DummyOperator(task_id='end', trigger_rule=TriggerRule.ONE_SUCCESS, dag=dag)

        subdag = get_discover_dag(id=f"discover-{collection['collection']}", event=processing_dag_template)

        start >> subdag >> end
        # end state should mark dataset as updated



###
#
#  Some assumptions:
#  -  automated collections are not complex - no datetime properties, no multi-asset, no CMR
#  -  the size and volume of incoming files each month (or on initial run) is relatively small (this is a convenience solution, not a scaling one)
#
###


