
import logging
import pendulum
from airflow.models.param import Param
from airflow.decorators import task
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.models.variable import Variable
from airflow.operators.python import PythonVirtualenvOperator
from veda_data_pipeline.groups.collection_group import ingest_collection_task

dag_doc_md = """
### Build and submit stac
#### Purpose
This DAG is supposed to be triggered by `veda_discover`. But you still can trigger this DAG manually or through an API

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
    "dashboard:is_timeless":"false",
    "temporal: {"interval" : [["2025-01-12T00:00:00+00:00", "2025-01-12T23:59:59+00:00"]] }

"""


template_conf = {
    "url": "",
    "id": "",
    "title": "",
    "stac_version": "",
    "description": "",
    "license": "",
    "dashboard:is_periodic": "",
    "dashboard:time_density": "",
    "dashboard:is_timeless":"",
    "temporal": {},
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

    Special handling is included for dashboard temporal flags, such as
    `dashboard:is_periodic` and `dashboard:is_timeless`, to ensure compatibility
    with VEDA rendering expectations.

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

    url =  template_conf.get("url") or event.get("url")
    if not url:
        raise ValueError(
            "URL is required but not provided in the event or template_conf."
        )

    # Generate STAC collection from ArcGIS Image/Map/Feature Server
    reader = ArcReader(server_url=url)
    collection = reader.generate_stac().to_dict()


    def _choose_keyValues(key, default):
        """
        Resolve a key's value using the following precedence:
        1. `template_conf` (highest priority)
        2. `event`
        3. `default` (fallback)

        This logic avoids Python's built-in truthy/falsey evaluation to preserve valid values
        like `False` or `0`, which are meaningful for flags such as `dashboard:is_timeless` 
        and `dashboard:is_periodic`.
        """
        value = template_conf.get(key)
        if value not in (None, ""):
            return value

        value = event.get(key)
        return value if value not in (None, "") else default

    def _temporal_extent_handling(template_conf,collection):
        """
        Override temporal extent flags for VEDA compatibility.

        If `dashboard:is_periodic` is explicitly True in `template_conf`, update the
        collection accordingly and remove `dashboard:is_timeless`, which may have been
        set by pyarc2stac when no start/end dates are found.
        """
        # Normalize the input (so "true"/"false" strings become booleans)
        raw = template_conf.get("dashboard:is_periodic")
        periodic = raw is True or (isinstance(raw, str) and raw.lower() == "true")

        if periodic:
            collection["dashboard:is_periodic"] = True
            # remove any timeless flag that pyarc2stac might have set
            collection.pop("dashboard:is_timeless", None)
        return collection

    # Overwrite keys based on order of precedence. User config in manual triggering is first in template_conf, followed by
    # values placed within the veda-tf-state-shared S3 bucket, and the last option is pyarc2stac generated values.
    for key in collection.keys():
        #Override with either spatial or temporal extents
        if key == 'extent':
            for ex_key, ex_val in collection['extent'].items():
                collection['extent'][ex_key] = (template_conf.get(ex_key) or event.get(ex_key) or ex_val) 
        else:
            # (optional) debug logging:
            print(f"Key: {key!r}, pyarc2stac: {collection[key]!r}, template_conf: {template_conf.get(key)!r}, event: {event.get(key)!r}")

            collection[key] = _choose_keyValues(key, collection[key])

            print(f"→ Final {key!r} = {collection[key]!r}")
        print(f"Final value for {key} is {collection.get(key)}")

    # Finalize special-case logic
    collection = _temporal_extent_handling(template_conf, collection)

    return collection



def get_ingest_pyarc2stac_dag(id: str, event: dict):
    with DAG(
            id,
            schedule=event.get("schedule", None), # schedule can be None for manual triggering
            render_template_as_native_obj=True,   # required to use params in the DAG
            params=template_conf,
            **dag_args
    ) as dag:
        start = EmptyOperator(task_id="Start", dag=dag)
        end = EmptyOperator(task_id="End", trigger_rule=TriggerRule.ONE_SUCCESS, dag=dag)

        convert = PythonVirtualenvOperator(
            task_id="pyarc2stac",
            python_callable=read_url_pyarc2stac_callable,
            requirements=["git+https://github.com/NASA-IMPACT/pyarc2stac.git@main#egg=pyarc2stac"],
            system_site_packages=False,
            op_kwargs={
                "event": event,
                "template_conf": "{{ params }}"
            },
            dag=dag
        )

        # Update task dependencies
        start >> convert >> ingest_collection_task(collection=convert.output) >> end

        return dag


# Sending empty event because we rely on task instance (ti) for manual runs
# and payload for scheduled runs
get_ingest_pyarc2stac_dag(id="veda_pyarc2stac_ingest", event={})