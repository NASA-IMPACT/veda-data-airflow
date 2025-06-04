import pendulum
from datetime import timedelta
import requests
import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.decorators import task, task_group
from veda_data_pipeline.groups.collection_group import ingest_collection_task

VIIRS_SNPP_NRT_collection = {
        "assets": {},
        "collection": "VIIRS_SNPP_DayNightBand_At_Sensor_Radiance",
        "dashboard:is_periodic": True,
        "dashboard:time_density": "day",
        "dashboard:time_interval": "P1D",
        "data_type": "cog",
        "description": "The Black Marble Nighttime At Sensor Radiance (Day/Night Band) layer is created from NASA’s Black Marble daily at-sensor top-of-atmosphere nighttime radiance product (VNP46A1). It is displayed as a grayscale image. The layer is expressed in radiance units (nW/(cm2 sr)) with log10 conversion. It is stretched up to 38 nW/(cm2 sr) resulting in improvements in capturing city lights in greater spatial detail than traditional Nighttime Imagery resampled at 0-255 (e.g., Day/Night Band, Enhanced Near Constant Contrast).The ultra-sensitivity of the VIIRS Day/Night Band enables scientists to capture the Earth’s surface and atmosphere in low light conditions, allowing for better monitoring of nighttime phenomena. These images are also useful for assessing anthropogenic sources of light emissions under varying illumination conditions. For instance, during partial to full moon conditions, the layer can identify the location and features of clouds and other natural terrestrial features such as sea ice and snow cover, while enabling temporal observations in urban regions, regardless of moonlit conditions. As such, the layer is particularly useful for detecting city lights, lightning, auroras, fires, gas flares, and fishing fleets.The Black Marble Nighttime At Sensor Radiance (Day/Night Band) layer is available in near real-time from the Visible Infrared Imaging Radiometer Suite (VIIRS) aboard the joint NASA/NOAA Suomi National Polar orbiting Partnership (Suomi NPP) satellite. The sensor resolution is 750 m at nadir, imagery resolution is 500 m, and the temporal resolution is daily.",
        "extent": {
            "spatial": {
                "bbox": [
                    [
                        -180,
                        -90,
                        180,
                        90
                    ]
                ]
            },
            "temporal": {
                "interval": [
                    [
                        "2020-11-10T00:00:00Z",
                        "2025-04-14T00:00:00Z"
                    ]
                ]
            }
        },
        "is_periodic": True,
        "item_assets": {
            "cog_default": {
                "description": "Cloud optimized default layer to display on map",
                "roles": [
                    "data",
                    "layer"
                ],
                "title": "Default COG Layer",
                "type": "image/tiff; application=geotiff; profile=cloud-optimized"
            }
        },
        "license": "MIT",
        "links": [
            {
                "href": "https://gibs.earthdata.nasa.gov/wmts/epsg3857/best/wmts.cgi",
                "href:servers": [
                    "https://gibs-a.earthdata.nasa.gov/wmts/epsg3857/best/wmts.cgi",
                    "https://gibs-b.earthdata.nasa.gov/wmts/epsg3857/best/wmts.cgi"
                ],
                "rel": "wmts",
                "title": "Visualized through a WMTS",
                "type": "image/png",
                "wmts:dimensions": [
                    "default"
                ],
                "wmts:layers": [
                    "VIIRS_SNPP_DayNightBand_At_Sensor_Radiance"
                ]
            }
        ],
        "product_level": "L2",
        "providers": [],
        "renders": {},
        "stac_extensions": [
            "https://stac-extensions.github.io/render/v1.0.0/schema.json",
            "https://stac-extensions.github.io/item-assets/v1.0.0/schema.json"
        ],
        "stac_version": "1.1.0",
        "temporal_frequency": "twenty four hours",
        "time_density": "day",
        "time_interval": "P1D",
        "title": "Black Marble Nighttime At Sensor Radiance (Day/Night Band)",
        "type": "Collection",
        "units": "m·s⁻¹"
    }

dag_doc_md = f"""
### This DAG handles VIIRS_SNPP_DayNightBand_At_Sensor_Radiance NRT dataset update.
It checks if the NRT data hosted by earthdata is avaialble for the latest available date
via. https://gibs.earthdata.nasa.gov/wmts/epsg4326/best/1.0.0/WMTSCapabilities.xml
If available, it overrides the VIIRS_SNPP_DayNightBand_At_Sensor_Radiance collection
with the updated temporal extent and ingests into the catalog.
#### Notes
- This DAG can uses the following configuration for VIIRS_SNPP_DayNightBand_At_Sensor_Radiance NRT collection <br>
```json
{VIIRS_SNPP_NRT_collection}
```
"""

dag_args = {
    "start_date": pendulum.today("UTC").add(days=-1),
    "catchup": False,
    "doc_md": dag_doc_md,
    "tags": ["collection"],
}

# Special task group to update nightlight NRT data collection that is pulled from worldview
@task_group(group_id="worldview_nightlight_nrt_collection_update_pipeline", tooltip="worldview nightlight NRT Collection update")
def worldview_collection_update_task_group(**context):
    nrt_collection = context.get("VIIRS_SNPP_NRT_collection")
    gibs_url: str ="https://gibs.earthdata.nasa.gov/wmts/epsg4326/best/1.0.0/WMTSCapabilities.xml"

    @task_group(tooltip="validate if collection update is needed via metadata available on gibs")
    def validation_task_group(gibs_url: str) -> dict:
        xml_string_data = fetch_nightlight_meta_from_gibs(gibs_url)
        latest_layer_date = extract_latest_nrt_date(xml_string_data)
        is_update_needed = nrt_update_check(latest_layer_date)
        return {
            "is_update_needed": is_update_needed,
            "latest_layer_date": latest_layer_date
        }

    @task_group(tooltip="update the nightlight nrt collection with the available nrt date")
    def collection_update_task_group(nrt_collection: dict, latest_layer_date: str) -> None:
        updated_collection = update_nrt_collection_task(nrt_collection, latest_layer_date)
        ingest_collection_task(collection=updated_collection)

    @task.branch
    def update_needed(update_needed: bool) -> str:
        """
        This task branches to either end or to upadte_nrt_collection_task,
        based on the provided string date "%Y-%m-%d".
        """
        if not update_needed:
            return 'end'
        else:
            return 'update_nrt_collection_task'

    validation_result = validation_task_group(gibs_url)
    branch_choice_instance = update_needed(validation_result['is_update_needed'])
    
    branch_choice_instance >> end
    branch_choice_instance >> collection_update_task_group(nrt_collection, validation_result['latest_layer_date'])

@task
def nrt_update_check(nrt_date: str="") -> bool:
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
def update_nrt_collection_task(previous_collection=None, latest_nrt_date=None):
    import copy

    if (not previous_collection or not latest_nrt_date):
        return None

    updated_collection = copy.deepcopy(previous_collection)
    year, month, day = map(int, latest_nrt_date.split("-"))
    latest_nrt_data_date = datetime.date(year, month, day)
    formatted_datetime = latest_nrt_data_date.strftime("%Y-%m-%dT00:00:00Z")
    updated_collection['extent']['temporal']['interval'][0][1] = formatted_datetime
    return updated_collection

@task
def fetch_nightlight_meta_from_gibs(gibs_url: str="https://gibs.earthdata.nasa.gov/wmts/epsg4326/best/1.0.0/WMTSCapabilities.xml") -> str:
    try:
        response = requests.get(gibs_url)
        response.raise_for_status()
        xml_string = response.text
        return xml_string
    except requests.exceptions.RequestException as e:
        print(f"Error fetching from the gibs: {e}")
        return ""

@task
def extract_latest_nrt_date(xml_string: str) -> str:
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

with DAG(
    "veda_worldview_nrt_data_collection_update",
    schedule="0 0 * * *",
    render_template_as_native_obj=True,
    **dag_args
) as dag:
    start = EmptyOperator(task_id="start", dag=dag)
    end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS, dag=dag)

    collection_grp = worldview_collection_update_task_group(VIIRS_SNPP_NRT_collection=VIIRS_SNPP_NRT_collection)

    start >> collection_grp >> end
