import logging
import pendulum
import traceback
import time
import json
from airflow import DAG
from airflow.exceptions import AirflowException
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
        description="List of collection IDs to tag. In the UI form, enter one collection ID per line. When triggering via API or CLI, provide as a list: [\"collection1\", \"collection2\"] (JSON format will be converted to a Python list)"
    ),
    "tenant": Param(default=None, type="string", description="Tenant ID to tag the collection with"),
    "tenant_field": Param(
        default="eic:tenant",
        type="string",
        description="Top-level collection key to write the tenant into (e.g., 'eic-tenant' or 'eic:tenant')",
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
        collections = config.get("collections")
        tenant = config.get("tenant")

        logger.info(f"Starting collection ID validation. Tenant: {tenant}")

        # Validate collections is a list
        if not isinstance(collections, list):
            error_msg = f"Collections must be a list, but got type: {type(collections)}. For UI form: enter one collection ID per line. For API or CLI: provide as a list [\"col1\", \"col2\"]"
            logger.error(error_msg)
            raise ValueError(error_msg)

        if len(collections) == 0:
            error_msg = "Collections list cannot be empty"
            logger.error(error_msg)
            raise ValueError(error_msg)

        # Validate and normalize collection IDs
        normalized_collections = []
        for coll in collections:
            if not isinstance(coll, str):
                error_msg = f"Collections must be a list of strings, but got element of type: {type(coll)}"
                logger.error(error_msg)
                raise ValueError(error_msg)

            coll_stripped = coll.strip()
            if not coll_stripped:
                continue

            # Handle case where user entered multiple values in a single string (fallback)
            if "\n" in coll_stripped or "," in coll_stripped:
                logger.warning(f"Received string with separators: {coll_stripped}. Parsing as fallback. For UI form: enter one collection ID per line (should be parsed automatically). For API/CLI: provide as a list [\"col1\", \"col2\"]")
                if "\n" in coll_stripped:
                    # split on newlines (to handle UI form input)
                    split_collections = [c.strip() for c in coll_stripped.split("\n") if c.strip()]
                else:
                    # split on commas (in case this is used in form)
                    split_collections = [c.strip() for c in coll_stripped.split(",") if c.strip()]

                # strip quotes from each
                for split_coll in split_collections:
                    cleaned = split_coll.strip('"').strip("'").strip()
                    if cleaned:
                        normalized_collections.append(cleaned)
            else:
                # basic case: single collection ID
                cleaned = coll_stripped.strip('"').strip("'").strip()
                if cleaned:
                    normalized_collections.append(cleaned)

        logger.info(f"Validated {len(normalized_collections)} collection IDs")
        logger.info(f"Returning collection IDs: {normalized_collections}")
        return normalized_collections

    except Exception as e:
        logger.error(f"Error in get_collection_ids: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise

@task()
def fetch_existing_collection(collection_id: str):
    """Fetch an existing collection from the STAC catalog. Returns None if collection doesn't exist or fetching returns error."""
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
            # If collection doesn't exist (404), log and return None instead of failing
            if response.status_code == 404:
                logger.warning(f"Collection {collection_id} not found (404). Skipping...")
                return None
            if response.status_code == 500:
                logger.warning(f"Collection {collection_id} returns an Internal Server Error. Skipping...")
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                logger.warning(f"Collection {collection_id} not found (404). Skipping...")
                return None
            error_msg = f"HTTP error while fetching collection {collection_id}: {str(e)}"
            logger.error(error_msg)
            raise
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
    """Update collection with tenant tags at the top level. Returns None if collection is None (doesn't exist)."""
    try:
        # Skip if collection doesn't exist (was None from fetch step)
        if existing_collection is None:
            logger.warning("Skipping update - collection does not exist")
            return None

        config = ti.dag_run.conf
        tenant = config.get("tenant")
        tenant_field = config.get("tenant_field") or "eic:tenant"

        collection_id = existing_collection.get("id") if existing_collection else "unknown"
        logger.info(f"Updating collection {collection_id} with tenant tags")

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

@task(retries=0)
def ingest_all_collections(collections=None):
    """Ingest all collections sequentially"""
    if not collections:
        logger.warning("No collections provided for ingestion")
        return []

    results = []
    total = len(collections)

    airflow_vars_json = Variable.get("aws_dags_variables", deserialize_json=True)
    app_secret = airflow_vars_json.get("INGEST_API_KEYCLOAK_APP_SECRET")
    stac_ingestor_api_url = airflow_vars_json.get("STAC_INGESTOR_API_URL")

    if not app_secret or not stac_ingestor_api_url:
        error_msg = "INGEST_API_KEYCLOAK_APP_SECRET or STAC_INGESTOR_API_URL not found in Airflow variables"
        logger.error(error_msg)
        raise ValueError(error_msg)

    for idx, collection in enumerate(collections, 1):
        # Skip None collections (collections that don't exist)
        if collection is None:
            logger.warning(f"Skipping ingestion - collection does not exist ({idx}/{total})")
            continue

        collection_id = collection.get("id") if collection else "unknown"

        # Adding logging to calculate and log payload size before sending to be ingested
        try:
            payload_json = json.dumps(collection)
            payload_size = len(payload_json.encode('utf-8'))
            payload_size_kb = payload_size / 1024
            logger.info(f"Starting ingestion of collection {collection_id} ({idx}/{total}) - payload size: {payload_size:,} bytes ({payload_size_kb:.2f} KB)")

            if payload_size > 8192:
                logger.warning(f"Collection {collection_id} payload size ({payload_size_kb:.2f} KB) exceeds 8KB - may trigger WAF SizeRestrictions_BODY rule")
        except Exception as e:
            logger.warning(f"Could not calculate payload size for {collection_id}: {str(e)}")
            payload_size = None
            payload_size_kb = None
            logger.info(f"Starting ingestion of collection {collection_id} ({idx}/{total})")

        try:
            submission_handler(
                event=collection,
                endpoint="/collections",
                app_secret=app_secret,
                stac_ingestor_api_url=stac_ingestor_api_url
            )
            logger.info(f"Successfully ingested collection {collection_id} ({idx}/{total})")
            results.append({"collection_id": collection_id, "status": "success", "payload_size_bytes": payload_size})
        except Exception as e:
            error_msg = str(e)
            if payload_size:
                logger.error(f"Error ingesting collection {collection_id}: {error_msg} - payload size: {payload_size:,} bytes ({payload_size_kb:.2f} KB)")
                if "403" in error_msg or "SizeRestrictions" in error_msg or "Request blocked" in error_msg:
                    logger.error(f"Collection {collection_id} was likely blocked by WAF due to payload size ({payload_size_kb:.2f} KB)")
            else:
                logger.error(f"Error ingesting collection {collection_id}: {error_msg}")
            results.append({"collection_id": collection_id, "status": "error", "error": error_msg, "payload_size_bytes": payload_size})
            # Continue processing other collections instead of failing immediately
            continue

        # I put this small delay between requests in case we need to avoid rate limiting
        if idx < total:
            time.sleep(0.5)

    successful = sum(1 for r in results if r.get("status") == "success")
    failed = sum(1 for r in results if r.get("status") == "error")
    logger.info(f"Ingestion complete: {successful} successful, {failed} failed out of {total} total")

    if failed > 0:
        failed_collections = [r["collection_id"] for r in results if r.get("status") == "error"]
        logger.warning(f"Failed collections: {failed_collections}")
        raise AirflowException(f"Failed to ingest {failed} collection(s): {failed_collections}")

    return results

with DAG("veda_tenant_tagging_pipeline", params=template_dag_run_conf, **dag_args) as dag:
    start = EmptyOperator(task_id="start", dag=dag)
    end = EmptyOperator(task_id="end", dag=dag)

    collection_ids = get_collection_ids()
    fetch_collections = fetch_existing_collection.expand(collection_id=collection_ids)
    update_collections = update_collection_with_tenant_tags.expand(existing_collection=fetch_collections)
    ingest_collections = ingest_all_collections(collections=update_collections)

    start >> collection_ids >> fetch_collections >> update_collections >> ingest_collections >> end