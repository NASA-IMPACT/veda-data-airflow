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
    xml_string = fetch_nightlight_meta_from_gibs()
    if (not xml_string):
        return

    latest_layer_date = extract_xml_date(xml_string)
    if (not latest_layer_date):
        return

    if nrt_collection and nrt_update_check_task(latest_layer_date) and (updated_collection := update_nrt_collection_task(nrt_collection)):
        ingest_collection_task(collection=updated_collection)

@task()
def nrt_update_check_task(ti=None, nrt_date: str="") -> bool:
    """
    Check if a Near Real-Time (NRT) data is updated to latest/today.

    This task compares a provided date string with the current date to determine
    if the NRT data is up-to-date. The nrt_date parameter should be in the format
    "%Y-%m-%d".
    """
    if (not nrt_date):
        return False

    year, month, day = map(int, nrt_date.split("-"))
    latest_nrt_data_date = datetime.date(year, month, day)
    today = datetime.date.today()
    if (latest_nrt_data_date >= today):
        return True
    return False

@task()
def update_nrt_collection_task(ti=None, previous_collection=None, latest_nrt_date=None):
    import copy

    if (not previous_collection or not latest_nrt_date):
        return None

    updated_collection = copy.deepcopy(previous_collection)
    year, month, day = map(int, latest_nrt_date.split("-"))
    latest_nrt_data_date = datetime.date(year, month, day)
    formatted_datetime = latest_nrt_data_date.strftime("%Y-%m-%dT00:00:00Z")
    updated_collection['extent']['temporal']['interval'][0][1] = formatted_datetime
    return updated_collection

# helper
def fetch_nightlight_meta_from_gibs(gibs_url: str="https://gibs.earthdata.nasa.gov/wmts/epsg4326/best/1.0.0/WMTSCapabilities.xml") -> str:
    try:
        response = requests.get(gibs_url)
        response.raise_for_status()
        xml_string = response.text
        return xml_string
    except requests.exceptions.RequestException as e:
        print(f"Error fetching from the gibs: {e}")
        return ""

def extract_xml_date(xml_string: str) -> str:
    import xml.etree.ElementTree as ET
    XML_NAMESPACE = {'xmlns': 'http://www.opengis.net/wmts/1.0'}
    OWS_NAMESPACE = {'ows': 'http://www.opengis.net/ows/1.1'}

    if not xml_string:
        return ""

    root = ET.fromstring(xml_string)
    contents = root.find('xmlns:Contents', XML_NAMESPACE)
    if contents is None:
        return False
    layers = contents.findall('xmlns:Layer', XML_NAMESPACE)
    if not layers:
        return False
    for layer in layers:
        layer_id = layer.find('ows:Identifier', OWS_NAMESPACE).text
        if (layer_id == 'VIIRS_SNPP_DayNightBand_At_Sensor_Radiance'):
            dimension = layer.find('xmlns:Dimension', XML_NAMESPACE)
            dimension_id = dimension.find('ows:Identifier', OWS_NAMESPACE).text
            if (dimension_id == 'Time'):
                layer_date = dimension.find('xmlns:Default', XML_NAMESPACE).text
                return layer_date
    return ""
