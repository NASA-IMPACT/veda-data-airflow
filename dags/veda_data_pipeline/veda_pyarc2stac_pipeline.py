
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
    "stac_id": "nrt_lis_ak_green_veg_fraction",
    "title": "NRT LIS Alaska Green Vegetation Fraction",
    "stac_version": "1.0.0",
    "description": "Insert description here",
    "data_type": "",
    "license": "CC1.0 Universal", 
    "dashboard:is_periodic": true,
    "dashboard:time_density": "day",

"""


template_conf = {
    "url": "",
    "id": "",
    "title": "",
    "stac_version": "",
    "description": "",
    "data_type": "",
    "license": "",
    "dashboard:is_periodic": "",
    "dashboard:time_density": ""
}


dag_args = {
    "start_date": pendulum.today("UTC").add(days=-1),
    "catchup": False,
    "doc_md": dag_doc_md,
}



def read_url_pyarc2stac_callable(event: dict, template_conf: dict) -> dict:
    """
    Generate a STAC collection from an ArcGIS server URL and merge with a template configuration.

    This function retrieves a URL from the provided `event` dictionary or, if not present,
    from the `template_dag_run_conf` dictionary. It then uses `pyarc2stac.ArcReader` to
    generate a STAC collection, converts it to a dictionary, and overwrites any keys with
    non-empty values from the `template_dag_run_conf`.

    Parameters
    ----------
    event : dict
        Dictionary containing runtime event parameters. Expected to include a key "url"
        pointing to the ArcGIS server endpoint.
    template_dag_run_conf : dict
        Dictionary of default STAC collection configuration values. Any key in this
        dictionary with a non-empty string value will overwrite the corresponding key
        in the generated STAC collection.

    Returns
    -------
    dict
        A STAC collection represented as a dictionary, with keys from `template_conf`
        merged in where values are non-empty.

    Raises
    ------
    ValueError
        If no URL is provided in either `event` or `template_conf`, a ValueError
        is raised indicating that the URL is required.

    Example
    -------
    >>> event = {"url": "https://example.com/arcgis/rest/services/MyService", "id": "my_collection_id", ... remaining key/values from AWS .json file}
    >>> template = {"title": "My Custom Title", "description": ""}
    >>> collection = read_url_pyarc2stac_callable(event, template)
    """
    from pyarc2stac.ArcReader import ArcReader

    url = event.get("url") or template_conf.get("url")
    if not url:
        raise ValueError(
            "URL is required but not provided in the event or template_conf."
        )

    # Retrieve data from the ArcGIS server URL
    reader = ArcReader(server_url=url)
    collection = reader.generate_stac().to_dict()

    # Overwrite keys based on order of precedence. User config in manual triggering is first in template_conf, followed by
    # values placed within the veda-tf-state-shared S3 bucket, and the last option is pyarc2stac generated values.
    for key in collection.keys():
        collection[key] = (template_conf.get(key) or event.get(key) or collection[key])

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