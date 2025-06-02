import requests
import datetime
from airflow.models.variable import Variable
from airflow.decorators import task, task_group

from veda_data_pipeline.utils.collection_generation import GenerateCollection
from veda_data_pipeline.utils.submit_stac import submission_handler

generator = GenerateCollection()


def check_collection_exists(endpoint: str, collection_id: str):
    """
    Check if a collection exists in the STAC catalog

    Args:
        endpoint (str): STAC catalog endpoint
        collection_id (str): collection id
    """
    response = requests.get(f"{endpoint}/collections/{collection_id}")
    return (
        "Collection.existing_collection"
        if (response.status_code == 200)
        else "Collection.generate_collection"
    )

@task()
def ingest_collection_task(ti=None, collection=None):
    """
    Ingest a collection into the STAC catalog

    Args:
        dataset (Dict[str, Any]): dataset dictionary (JSON)
        role_arn (str): role arn for Zarr collection generation
    """
    import json
    if not collection:
        collection = ti.xcom_pull(task_ids='Collection.generate_collection')
    airflow_vars_json = Variable.get("aws_dags_variables", deserialize_json=True)
    app_secret = airflow_vars_json.get("INGEST_API_KEYCLOAK_APP_SECRET")
    stac_ingestor_api_url = airflow_vars_json.get("STAC_INGESTOR_API_URL")

    return submission_handler(
        event=collection,
        endpoint="/collections",
        app_secret=app_secret,
        stac_ingestor_api_url=stac_ingestor_api_url
    )


# NOTE unused, but useful for item ingests, since collections are a dependency for items
def check_collection_exists_task(ti=None):
    import json
    config = ti.dag_run.conf
    airflow_vars = Variable.get("aws_dags_variables")
    airflow_vars_json = json.loads(airflow_vars)
    stac_url = airflow_vars_json.get("STAC_URL")
    return check_collection_exists(
        endpoint=stac_url,
        collection_id=config.get("collection"),
    )


@task()
def generate_collection_task(ti=None):
    import json
    config = ti.dag_run.conf
    airflow_vars = Variable.get("aws_dags_variables")
    airflow_vars_json = json.loads(airflow_vars)
    role_arn = airflow_vars_json.get("ASSUME_ROLE_READ_ARN")

    # TODO it would be ideal if this also works with complete collections where provided - this would make the collection ingest more re-usable
    collection = generator.generate_stac(
        dataset_config=config, role_arn=role_arn
    )
    return collection

@task_group(group_id="Collection", tooltip="Collection")
def collection_task_group():
    generate_collection = generate_collection_task()
    ingest_collection = ingest_collection_task(collection=generate_collection)

# Special task group to update nightlight NRT data collection that is pulled from worldview
@task_group(group_id="Worldview nightlight NRT Collection update pipeline", tooltip="worldview nightlight NRT Collection update")
def worldview_collection_update_task_group(**context):
    nrt_collection = context.get("VIIRS_SNPP_NRT_collection")
    if nrt_collection and nrt_update_check_task() and (updated_collection := update_nrt_collection_task(nrt_collection)):
        ingest_collection_task(collection=updated_collection)

@task()
def nrt_update_check_task(ti=None):
    import xml.etree.ElementTree as ET
    try:
        gibs_url = "https://gibs.earthdata.nasa.gov/wmts/epsg4326/best/1.0.0/WMTSCapabilities.xml"
        response = requests.get(gibs_url)
        response.raise_for_status()
        xml_string = response.text
        if (not xml_string):
            return False
        root = ET.fromstring(xml_string)
        for layer in root.iter("Layer"):
            if (layer.find("ows:Identifier").text == "VIIRS_SNPP_DayNightBand_At_Sensor_Radiance"):
                dimension = layer.get("Dimension")
                latest_layer_date = dimension.find("default")
                year, month, day = map(int, latest_layer_date.split("-"))
                test_date = datetime.date(year, month, day)
                today = datetime.date.today()
                if (test_date >= today):
                    return True
        return False
    except requests.exceptions.RequestException as e:
        print(f"Error fetching from the gibs: {e}")
        return False

@task()
def update_nrt_collection_task(ti=None, previous_collection=None):
    if (not previous_collection):
        return None
    now = datetime.datetime.now()
    formatted_datetime = now.strftime("%Y-%m-%dT00:00:00Z")
    updated_collection = previous_collection['extent']['temporal']['interval'][1] = formatted_datetime
    return updated_collection
