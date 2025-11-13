import pendulum
from datetime import timedelta
import requests
import datetime
from dataclasses import dataclass
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.decorators import dag, task, task_group
from veda_data_pipeline.groups.collection_group import ingest_collection_task

CollectionConfig = dict[str, any] # this mostly comply with a STAC json config

@dataclass
class NRTCollectionUpdateConfig:
    collection_id: str # Collection_id in STAC
    gibs_url: str
    collection_config: CollectionConfig

# Example Collection Config. Used as a default value.
VIIRS_SNPP_NRT_collection: CollectionConfig = {
        "assets": {},
        "id": "VIIRS_SNPP_DayNightBand_At_Sensor_Radiance",
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
            "https://stac-extensions.github.io/item-assets/v1.0.0/schema.json",
            "https://stac-extensions.github.io/web-map-links/v1.2.0/schema.json"
        ],
        "stac_version": "1.1.0",
        "temporal_frequency": "twenty four hours",
        "time_density": "day",
        "time_interval": "P1D",
        "title": "Black Marble Nighttime At Sensor Radiance (Day/Night Band)",
        "type": "Collection",
        "units": "m·s⁻¹"
    }

# Example NRT Collection Update Config. Used as a default value.
veda_worldview_nrt_data_collection_update_dag_creator_config: NRTCollectionUpdateConfig = {
    "collection_id": "VIIRS_SNPP_DayNightBand_At_Sensor_Radiance",
    "gibs_url": "https://gibs.earthdata.nasa.gov/wmts/epsg4326/best/1.0.0/WMTSCapabilities.xml",
    "collection_config": VIIRS_SNPP_NRT_collection
}

def veda_worldview_nrt_data_collection_update_dag_creator(id: str, event: NRTCollectionUpdateConfig):
    """
    A wrapper function that creates the veda_worldview_nrt_data_collection_update dags for specific collection
    :param id: Id for the DAG. should be unique
    : param event: A config dict 
    """
    collection_id = event["collection_id"]
    gibs_url = event["gibs_url"]
    collection_config = event["collection_config"]
    dag_doc_md = f"""
        ### This DAG handles {collection_id} NRT dataset update.
        It checks if the NRT data hosted by earthdata is avaialble for the latest available date
        via. {gibs_url}
        If available, it overrides the {collection_id} collection
        with the updated temporal extent and ingests into the STAC.
        #### Note
        - This DAG uses the following configuration json for {collection_id} NRT collection <br>
        ```json
        {collection_config}
        ```
        """
    dag_args = {
        "start_date": pendulum.today("UTC").add(days=-1),
        "catchup": False,
        "doc_md": dag_doc_md,
        "tags": ["collection", "update", "NRT", "worldview", "gibs"],
    }

    @dag(
        dag_id=id,
        schedule="0 0 * * *",
        render_template_as_native_obj=True,
        **dag_args
    )
    def veda_worldview_nrt_data_collection_update(collection_config: dict, collection_id: str, gibs_url: str):
        start = EmptyOperator(task_id="start")
        end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)

        # TASK DEFINATION START

        @task_group(group_id="worldview_collection_update_task_group", tooltip="worldview nightlight NRT Collection update")
        def worldview_collection_update_task_group(nrt_collection: dict, collection_id: str, gibs_url: str) -> None:
            """
            Task group to manage the update of the Nightlight Near Real-Time (NRT) data collection sourced from Worldview.

            This task group consists of tasks that:
            1. Validate if an update is needed by comparing the latest available data on GIBS with the current collection.
            2. If an update is needed, it updates the NRT collection with the latest available date.
            3. Finally, it ingests the updated collection.

            The sub-tasks are grouped into validation and collection update task groups for better organization and readability.
            A branching task determines whether to proceed with the update based on the validation result.
            :param nrt_collection: A collection config dictionary.
            :param gibs_url: The URL to the GIBS metadata endpoint.
            :param collection_id: The id of the collection in the GIBS.
            :return: None
            """
            @task_group(group_id="validation_task_group", tooltip="validate if collection update is needed via metadata available on gibs")
            def validation_task_group(gibs_url: str, collection_id: str) -> dict:
                """
                Task group to validate if a collection update is needed based on metadata from GIBS.

                This group fetches metadata from GIBS which is in XML format, extracts the latest date, and checks if an update is needed.

                :param gibs_url: The URL to the GIBS metadata endpoint.
                :param collection_id: The id of the collection in the GIBS.
                :return: A dictionary containing a boolean indicating if an update is needed and the latest layer date.
                """
                xml_string_data = fetch_metadata_from_gibs(gibs_url)
                latest_layer_date = extract_latest_nrt_date_for_collection(xml_string_data, collection_id)
                is_update_needed = nrt_update_check(latest_layer_date)
                # the above TaskFlow API implementation represents: fetch_metadata_from_gibs >> extract_latest_nrt_date_for_collection >> nrt_update_check
                return {
                    "is_update_needed": is_update_needed,
                    "latest_layer_date": latest_layer_date
                }

            @task_group(group_id="collection_update_task_group", tooltip="update the nightlight nrt collection with the available nrt date")
            def collection_update_task_group(nrt_collection: dict, latest_layer_date: str) -> None:
                """Updates the NRT collection config json with the latest available date and ingests it.

                :param nrt_collection: Dictionary representing the NRT collection config.
                :param latest_layer_date: The latest date for which NRT data is available.
                :return: None
                """
                updated_collection = update_nrt_collection_task(nrt_collection, latest_layer_date)
                ingest_collection_task(collection=updated_collection)

            @task.branch
            def branch_update_needed(update_needed: bool) -> str:
                """
                This task branches to either end or to upadte_nrt_collection_task,
                based on the provided boolean representing if updated is needed or not.
                :param update_needed: Boolean
                """
                if not update_needed:
                    return 'end'
                else:
                    return 'worldview_collection_update_task_group.collection_update_task_group.update_nrt_collection_task'

            validation_result = validation_task_group(gibs_url, collection_id)
            branch_choice_instance = branch_update_needed(validation_result['is_update_needed'])
            branch_choice_instance >> [end, collection_update_task_group(nrt_collection, validation_result['latest_layer_date'])]

        @task
        def nrt_update_check(nrt_date: str="") -> bool:
            """
            Check if Near Real-Time (NRT) data is up-to-date.

            Compares a provided date string with the current date to determine
            if the NRT data is up-to-date. The nrt_date parameter should be in the format
            "%Y-%m-%d". If nrt_date is empty, it returns False.

            :param nrt_date: Date string in the format "%Y-%m-%d" representing the latest NRT data date.
            :type nrt_date: str, optional
            :return: True if the NRT data is up-to-date (nrt_date is today or a future date), False otherwise.
            :rtype: bool
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
            """
            Updates the temporal extent of a collection with the latest NRT (Near Real-Time) data date.

            This task takes the previous collection metadata and the latest available date for NRT data,
            and updates the collection's temporal extent to include this new data.

            :param previous_collection: The previous collection config as a dictionary.
            :type previous_collection: dict, optional
            :param latest_nrt_date: The latest date for which NRT data is available, in 'YYYY-MM-DD' format.
                                    Defaults to None.
            :type latest_nrt_date: str, optional
            :return: The updated collection config with the temporal extent updated to include the
                    latest NRT data date.
            :rtype: dict or None
            """
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
        def fetch_metadata_from_gibs(gibs_url: str="https://gibs.earthdata.nasa.gov/wmts/epsg4326/best/1.0.0/WMTSCapabilities.xml") -> str:
            """
            Fetches the nightlight metadata from the NASA Global Imagery Browse Services (GIBS) server.

            Args:
                gibs_url (str): The URL for the GIBS WMTSCapabilities.xml endpoint.
                                Defaults to the public NASA GIBS endpoint for EPSG:4326.

            Returns:
                str: A string containing the XML response from the GIBS server,
                    or an empty string if the fetch fails.

            Raises:
                requests.exceptions.RequestException: If the HTTP request to the GIBS server fails.
            """
            try:
                response = requests.get(gibs_url)
                response.raise_for_status()
                xml_string = response.text
                return xml_string
            except requests.exceptions.RequestException as e:
                print(f"Error fetching from the gibs: {e}")
                return ""

        @task
        def extract_latest_nrt_date_for_collection(xml_string: str, collection_id:str='VIIRS_SNPP_DayNightBand_At_Sensor_Radiance') -> str:
            """
            Extracts the latest date from the WMTS GetCapabilities XML response for a specified layer.

            This function parses an XML string to find the most recent date available for a given
            collection ID within the WMTS GetCapabilities response. It uses the XML namespace
            to correctly locate the relevant elements and attributes.

            :param xml_string: A string containing the XML response from the WMTS GetCapabilities request.
            :type xml_string: str
            :param collection_id: The identifier of the layer for which to extract the latest date.
                Defaults to 'VIIRS_SNPP_DayNightBand_At_Sensor_Radiance'.
            :type collection_id: str, optional
            :return: The latest date as a string, extracted from the XML. Returns an empty string if the XML is empty,
                    or if the specified layer or date information is not found.
            :rtype: str
            :raises ET.ParseError: If the `xml_string` is not a valid XML.
            """
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
                if (layer_id == collection_id):
                    dimension = layer.find('xmlns:Dimension', XML_NAMESPACE)
                    dimension_id = dimension.find('ows:Identifier', OWS_NAMESPACE).text
                    if (dimension_id == 'Time'):
                        layer_date = dimension.find('xmlns:Default', XML_NAMESPACE).text
                        return layer_date
            return ""

        # TASK DEFINATION END

        collection_grp = worldview_collection_update_task_group(nrt_collection=collection_config, gibs_url=gibs_url, collection_id=collection_id)
        start >> collection_grp >> end

    veda_worldview_nrt_data_collection_update(collection_config=collection_config, collection_id=collection_id, gibs_url=gibs_url)

veda_worldview_nrt_data_collection_update_dag_creator(id="veda_worldview_nrt_data_collection_update_nightlight", event=veda_worldview_nrt_data_collection_update_dag_creator_config)
