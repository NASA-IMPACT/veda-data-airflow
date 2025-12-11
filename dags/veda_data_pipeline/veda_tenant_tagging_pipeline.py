import pendulum
from airflow import DAG
from airflow.models.param import Param
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from veda_data_pipeline.groups.collection_group import ingest_collection_task
from slack_notifications import slack_fail_alert
import requests
from airflow.models.variable import Variable

template_dag_run_conf = {
    "collection": Param(default=None, type="string", description="Collection ID to tag"),
    "tenant": Param(default=None, type="string", description="Tenant ID to tag the collection with (will be set as eic-tenant property)"),
    "properties": Param(
        default=None,
        type=["null", "object"],
        description="Additional properties to add/update on the collection (optional, tenant can also be set directly via 'tenant' parameter)"
    ),
}

dag_doc_md = """
### Tenant Tagging DAG
Tags an existing collection with tenant information by updating its properties field.

This pipeline:
1. Fetches the existing collection from the STAC catalog
2. Updates the collection's properties with tenant tags
3. Re-ingests the updated collection

#### Notes
- This DAG can run with the following configuration <br>
```json
{
    "collection": "collection-id",
    "tenant": "tenant-id"
}
```

Or with additional properties:
```json
{
    "collection": "collection-id",
    "tenant": "tenant-id",
    "properties": {
        "custom-property": "value"
    }
}
```
"""

dag_args = {
    "start_date": pendulum.today("UTC").add(days=-1),
    "schedule": None,
    "catchup": False,
    "doc_md": dag_doc_md,
    "on_failure_callback": slack_fail_alert,
    "tags": ["collection", "tenant"],
}

@task()
def fetch_existing_collection(ti=None):
    """ Fetch an existing collection from the STAC catalog"""
    import json
    config = ti.dag_run.conf
    collection_id = config.get("collection")

    if not collection_id:
        raise ValueError("Collection ID is required")

    airflow_vars_json = Variable.get("aws_dags_variables", deserialize_json=True)
    stac_url = airflow_vars_json.get("STAC_URL")

    if not stac_url:
        raise ValueError("STAC_URL not found in Airflow variables")

    response = requests.get(f"{stac_url.rstrip('/')}/collections/{collection_id}")
    response.raise_for_status()

    collection = response.json()
    return collection

@task()
def update_collection_with_tenant_tags(ti=None, existing_collection=None):
    """Update collection properties with tenant tags"""
    config = ti.dag_run.conf
    tenant = config.get("tenant")
    additional_properties = config.get("properties", {})

    if not existing_collection:
        raise ValueError("Existing collection is required")

    if not tenant:
        raise ValueError("Tenant ID is required. Please provide a 'tenant' parameter in the DAG configuration.")

    updated_collection = existing_collection.copy()

    if "properties" not in updated_collection:
        updated_collection["properties"] = {}

    updated_collection["properties"]["eic-tenant"] = tenant

    if additional_properties:
        updated_collection["properties"].update(additional_properties)

    return updated_collection

with DAG("veda_tenant_tagging_pipeline", params=template_dag_run_conf, **dag_args) as dag:
    start = EmptyOperator(task_id="start", dag=dag)
    end = EmptyOperator(task_id="end", dag=dag)

    fetch_collection = fetch_existing_collection()
    update_collection = update_collection_with_tenant_tags(existing_collection=fetch_collection)
    ingest_collection = ingest_collection_task(collection=update_collection)

    start >> fetch_collection >> update_collection >> ingest_collection >> end