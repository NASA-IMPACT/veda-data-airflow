import pendulum
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonVirtualenvOperator
from airflow.sdk import DAG
from airflow.sdk.definitions.param import Param
from airflow.utils.trigger_rule import TriggerRule
from veda_data_pipeline.groups.collection_group import ingest_collection_task

dag_doc_md = """
### Build and submit stac
#### Purpose
This DAG is supposed to be triggered by `veda_discover`. But you still can trigger this
DAG manually or through an API

#### Notes
- This DAG can run with a configuration similar to: <br>
```json
{
    "url": "https://maps.disasters.nasa.gov/ags03/rest/services/NRT/lis_ak_green_veg_fraction/ImageServer",
    "id": "nrt_lis_ak_green_veg_fraction",
    "title": "NRT LIS Alaska Green Vegetation Fraction",
    "stac_version": "1.0.0",
    "description": "Insert description here",
    "license": "CC1.0 Universal",
    "dashboard:is_periodic": true,
    "dashboard:time_density": "day",
    "temporal": {
        "interval": [["2025-01-12T00:00:00+00:00", "2025-01-12T23:59:59+00:00"]]
    }
}
```
"""


template_conf = {
    "url": Param(
        default=None,
        type=["null", "string"],
        description="ArcGIS Image|Map|Feature Server URL",
    ),
    "id": Param(
        default=None,
        type=["null", "string"],
        description="Collection ID within VEDA STAC",
    ),
    "title": Param(
        default=None, type=["null", "string"], description="Collection title"
    ),
    "description": Param(
        default=None, type=["null", "string"], description="Collection description"
    ),
    "stac_version": Param(
        default=None, type=["null", "string"], description="STAC version"
    ),
    "license": Param(default=None, type=["null", "string"], description="Data license"),
    "dashboard:is_periodic": Param(
        default=None,
        type=["null", "boolean", "string"],
        description="Is data periodic: Bool (True|False)",
    ),
    "dashboard:time_density": Param(
        default=None,
        type=["null", "string"],
        description="Time density: (day, month, year)",
    ),
    "temporal": Param(
        default=None, type=["null", "object"], description="Temporal extent"
    ),
}


dag_args = {
    "start_date": pendulum.today("UTC").add(days=-1),
    "catchup": False,
    "doc_md": dag_doc_md,
}


def read_url_pyarc2stac_callable(event: dict, template_conf: dict) -> dict:
    """
    Generate a STAC collection from an ArcGIS ImageServer URL using `pyarc2stac`,
    and merge it with a user-provided template configuration.

    The function uses a key precedence strategy to determine final values:
    1. `template_conf` — manual DAG trigger config (highest priority)
    2. `event` — JSON payload from S3 (medium priority)
    3. `pyarc2stac` generated values (fallback)

    Parameters
    ----------
    event : dict
        Runtime parameters, usually from an S3-hosted JSON payload.
    template_conf : dict
        Template configuration provided via manual DAG triggering or defaults.

    Returns
    -------
    dict
        A STAC collection dictionary with merged and sanitized configuration.

    Raises
    ------
    ValueError
        If no URL is found in either `event` or `template_conf`.
    """
    from pyarc2stac.ArcReader import ArcReader

    # Get URL from either source
    url = template_conf.get("url") or event.get("url")
    if not url:
        raise ValueError(
            "URL is required but not provided in the event or template_conf."
        )

    # Generate STAC collection from ArcGIS Image/Map/Feature Server
    reader = ArcReader(server_url=url)
    collection = reader.generate_stac().to_dict()

    # Create merged configuration with proper precedence
    # Filter out None and empty string values from configs
    filtered_template = {k: v for k, v in template_conf.items() if v not in (None, "")}
    filtered_event = {k: v for k, v in event.items() if v not in (None, "")}

    # Merge with precedence: template_conf > event > pyarc2stac defaults
    # Start with collection (pyarc2stac defaults), update with event, then template
    merged = collection.copy()

    # Handle temporal extent separately if it exists in configs.
    # This is useful for items with no temporal extent
    # in the initial pyarc2stac item creation
    if "temporal" in filtered_event:
        merged["extent"]["temporal"] = filtered_event["temporal"]
    if "temporal" in filtered_template:
        merged["extent"]["temporal"] = filtered_template["temporal"]

    # Update with event and template configs
    merged.update(filtered_event)
    merged.update(filtered_template)
    merged.pop(
        "dashboard:is_timeless", None
    )  # we do not want dashboard:is_timeless. Temporal extent should be specified.

    return merged


def get_ingest_pyarc2stac_dag(id: str, event: dict):
    with DAG(
        id,
        schedule=event.get("schedule"),  # schedule can be None for manual triggering
        render_template_as_native_obj=True,  # required to use params in the DAG
        params=template_conf,
        **dag_args,
    ) as dag:
        start = EmptyOperator(task_id="start", dag=dag)
        end = EmptyOperator(
            task_id="end", trigger_rule=TriggerRule.ONE_SUCCESS, dag=dag
        )

        pyarc2stac = PythonVirtualenvOperator(
            task_id="pyarc2stac",
            python_callable=read_url_pyarc2stac_callable,
            requirements=[
                "git+https://github.com/NASA-IMPACT/pyarc2stac.git@main#egg=pyarc2stac"
            ],
            system_site_packages=False,
            op_kwargs={"event": event, "template_conf": "{{ params }}"},
            dag=dag,
        )

        # Update task dependencies
        (
            start
            >> pyarc2stac
            >> ingest_collection_task(collection=pyarc2stac.output)
            >> end
        )

        return dag


# Sending empty event because we rely on task instance (ti) for manual runs
# and payload for scheduled runs
# get_ingest_pyarc2stac_dag(id="veda_pyarc2stac_ingest", event={})
