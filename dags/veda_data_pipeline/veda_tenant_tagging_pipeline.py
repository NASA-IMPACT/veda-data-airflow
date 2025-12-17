import logging
import pendulum
import traceback
from airflow import DAG
from airflow.models.param import Param
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from veda_data_pipeline.utils.submit_stac import submission_handler
from veda_data_pipeline.utils.schemas import normalize_temporal_extent
from slack_notifications import slack_fail_alert
import requests
from airflow.models.variable import Variable

logger = logging.getLogger(__name__)

template_dag_run_conf = {
    "collections": Param(
        default=None,
        type=["null", "array"],
        description="List of collection IDs to tag"
    ),
    "tenant": Param(default=None, type="string", description="Tenant ID to tag the collection with"),
    "tenant_field": Param(
        default="eic:tenant",
        type="string",
        description="Top-level collection key to write the tenant into (e.g., 'eic-tenant' or 'eic:tenant')",
    ),
    "properties": Param(
        default=None,
        type=["null", "object"],
        description="Additional properties to add/update on the collection (optional, tenant can also be set directly via 'tenant' parameter)"
    ),
}

dag_doc_md = """
### Tenant Tagging DAG
Tags existing collections with tenant information by updating the collection with tenant tags.

This pipeline:
1. Fetches existing collections from the STAC catalog
2. Updates each collection with tenant tags at the top level
3. Re-ingests the updated collections

#### Configuration

**Required Parameters:**
- `tenant` (string): Tenant ID to tag collections with

**Collection Source:**
- `collections` (array of strings): List of collection IDs to tag
**Optional Parameters:**
- `tenant_field` (string): Top-level collection key to write tenant into (default: `eic:tenant`)
- `properties` (object): Additional properties to add/update on collections (nested in properties field)

#### Example Configurations

**Tag specific collections:**
```json
{
    "collections": ["collection-id-1", "collection-id-2"],
    "tenant": "tenant-123"
}
```

**Tag specific collections using a custom tenant field:**
```json
{
    "collections": ["collection-id-1"],
    "tenant": "tenant-123",
    "tenant_field": "eic:tenant"
}
```


**With additional properties:**
```json
{
    "collections": ["collection-id-1"],
    "tenant": "tenant-123",
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
def get_collection_ids(ti=None):
    """Extract and validate collection IDs from configuration"""
    try:
        config = ti.dag_run.conf
        collections_strings = config.get("collections")[0]
        collections = collections_strings.split(",")
        tenant = config.get("tenant")

        logger.info(f"Starting collection ID validation. Tenant: {tenant}")

        if not collections:
            error_msg = "The 'collections' list must be provided in DAG configuration"
            logger.error(error_msg)
            raise ValueError(error_msg)

        if not isinstance(collections, list):
            error_msg = f"Collections must be a list, but got type: {type(collections)}"
            logger.error(error_msg)
            raise ValueError(error_msg)

        if len(collections) == 0:
            error_msg = "Collections list cannot be empty"
            logger.error(error_msg)
            raise ValueError(error_msg)

        # Validate and normalize collection IDs
        normalized_collections = []
        for coll in collections:
            logger.info(f"Looking at collection {coll}")
            if not isinstance(coll, str) or not coll.strip():
                raise ValueError(f"Collections must be non-empty strings, got: {coll}")
            normalized_collections.append(coll.strip())

        logger.info(f"Validated {len(normalized_collections)} collection IDs")
        return normalized_collections

    except Exception as e:
        logger.error(f"Error in get_collection_ids: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise

@task()
def fetch_existing_collection(collection_id: str):
    """Fetch an existing collection from the STAC catalog"""
    try:
        logger.info(f"Fetching collection: {collection_id}")

        airflow_vars_json = Variable.get("aws_dags_variables", deserialize_json=True)
        stac_url = airflow_vars_json.get("STAC_URL")

        if not stac_url:
            error_msg = "STAC_URL not found in Airflow variables"
            logger.error(error_msg)
            raise ValueError(error_msg)

        collection_url = f"{stac_url.rstrip('/')}/collections/{collection_id}"
        logger.debug(f"Requesting collection from: {collection_url}")

        try:
            response = requests.get(collection_url, timeout=30)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            error_msg = f"Request error while fetching collection {collection_id}: {str(e)}"
            logger.error(error_msg)
            raise

        if not response.text or not response.text.strip():
            error_msg = f"Empty response body for collection {collection_id}"
            logger.error(error_msg)
            logger.error(f"Response status code: {response.status_code}")
            raise ValueError(error_msg)

        try:
            collection = response.json()
        except (ValueError, requests.exceptions.JSONDecodeError) as json_error:
            error_msg = f"Failed to parse JSON response for collection {collection_id}"
            logger.error(error_msg)
            logger.error(f"Response status code: {response.status_code}")
            raise ValueError(f"{error_msg}. Response was not valid JSON. Status: {response.status_code}") from json_error

        logger.info(f"Successfully fetched collection {collection_id}")
        logger.debug(f"Collection keys: {list(collection.keys())}")
        return collection

    except Exception as e:
        logger.error(f"Error fetching collection {collection_id}: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise

@task()
def update_collection_with_tenant_tags(ti=None, existing_collection=None):
    """Update collection with tenant tags at the top level"""
    try:
        config = ti.dag_run.conf
        tenant = config.get("tenant")
        tenant_field = config.get("tenant_field") or "eic:tenant"
        additional_properties = config.get("properties", {})

        collection_id = existing_collection.get("id") if existing_collection else "unknown"
        logger.info(f"Updating collection {collection_id} with tenant tags")

        if not existing_collection:
            error_msg = "Existing collection is required but was not provided"
            logger.error(error_msg)
            raise ValueError(error_msg)

        if not tenant:
            error_msg = "Tenant ID is required. Please provide a 'tenant' parameter in the DAG configuration."
            logger.error(error_msg)
            raise ValueError(error_msg)

        if not isinstance(tenant_field, str) or not tenant_field.strip():
            error_msg = (
                "Tenant field is required and must be a non-empty string. "
                "Please provide a 'tenant_field' parameter in the DAG configuration."
            )
            logger.error(error_msg)
            raise ValueError(error_msg)
        tenant_field = tenant_field.strip()

        logger.debug(f"Existing collection keys: {list(existing_collection.keys())}")

        updated_collection = existing_collection.copy()

        if "properties" not in updated_collection:
            logger.debug(f"Collection {collection_id} has no properties field, creating one")
            updated_collection["properties"] = {}

        if additional_properties:
            logger.debug(f"Adding additional properties to collection {collection_id}: {additional_properties}")
            updated_collection["properties"].update(additional_properties)

        old_tenant = updated_collection.get(tenant_field)
        updated_collection[tenant_field] = tenant

        if old_tenant:
            logger.info(
                f"Collection {collection_id}: Updated {tenant_field} from '{old_tenant}' to '{tenant}'"
            )
        else:
            logger.info(f"Collection {collection_id}: Added {tenant_field} '{tenant}'")

        # Normalize temporal extent to ISO 8601 format
        updated_collection = normalize_temporal_extent(updated_collection)

        logger.info(f"Successfully updated collection {collection_id} with tenant tags")
        return updated_collection

    except Exception as e:
        collection_id = existing_collection.get("id") if existing_collection else "unknown"
        logger.error(f"Error updating collection {collection_id} with tenant tags: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise

@task()
def ingest_collection(collection=None):
    """Ingest collection"""
    collection_id = collection.get("id") if collection else "unknown"
    logger.info(f"Starting ingestion of collection {collection_id}")

    airflow_vars_json = Variable.get("aws_dags_variables", deserialize_json=True)
    app_secret = airflow_vars_json.get("INGEST_API_KEYCLOAK_APP_SECRET")
    stac_ingestor_api_url = airflow_vars_json.get("STAC_INGESTOR_API_URL")

    return submission_handler(
        event=collection,
        endpoint="/collections",
        app_secret=app_secret,
        stac_ingestor_api_url=stac_ingestor_api_url
    )

with DAG("veda_tenant_tagging_pipeline", params=template_dag_run_conf, **dag_args) as dag:
    start = EmptyOperator(task_id="start", dag=dag)
    end = EmptyOperator(task_id="end", dag=dag)

    collection_ids = get_collection_ids()
    fetch_collections = fetch_existing_collection.expand(collection_id=collection_ids)
    update_collections = update_collection_with_tenant_tags.expand(existing_collection=fetch_collections)
    ingest_collections = ingest_collection.expand(collection=update_collections)

    start >> collection_ids >> fetch_collections >> update_collections >> ingest_collections >> end