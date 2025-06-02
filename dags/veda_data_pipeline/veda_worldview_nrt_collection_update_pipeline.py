import pendulum
from datetime import timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from veda_data_pipeline.groups.collection_group import worldview_collection_update_task_group


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

with DAG(
    "Veda worldview NRT data collection update",
    schedule_interval=timedelta(days=1),
    render_template_as_native_obj=True,
    **dag_args
) as dag:
    start = EmptyOperator(task_id="start", dag=dag)
    end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS, dag=dag)

    collection_grp = worldview_collection_update_task_group(VIIRS_SNPP_NRT_collection=VIIRS_SNPP_NRT_collection)

    start >> collection_grp >> end
