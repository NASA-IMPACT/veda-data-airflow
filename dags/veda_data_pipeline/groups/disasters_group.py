"""Post-discovery metadata enrichment for the disasters use case.

Discovery writes its output to JSON payload files in EVENT_BUCKET and returns the
keys of those files. This group reads those payloads back, annotates each item's
properties with metadata read from the item's GeoTIFF tags (or its S3 path), and
writes the payload back to the same key so the rest of the pipeline is unchanged.

Keeping this out of `s3_discovery` means discovery stays generic - only DAGs that
opt in pay the cost of opening every discovered file.
"""

import json

import smart_open
from airflow.decorators import task

from veda_data_pipeline.utils.disasters_utils import (
    extract_all_metadata_from_geotiff,
    extract_event_name_from_geotiff,
    extract_providers_from_geotiff,
    extract_sensor_and_product_from_path,
)

group_kwgs = {"group_id": "Disasters", "tooltip": "Disasters"}

# Named extractors selectable from a DAG run config. Each takes the href of the
# item's first asset and returns a dict of STAC properties to merge in.
EXTRACTORS = {
    # Superset of `event_name`: also adds monty country/hazard/corr_id codes and
    # the hazard/location arrays.
    "monty": extract_all_metadata_from_geotiff,
    "event_name": extract_event_name_from_geotiff,
    "product": extract_sensor_and_product_from_path,
    "providers": extract_providers_from_geotiff,
}


def enrich_item(item: dict, extractors: list) -> int:
    """Merge extracted metadata into a single discovered item's properties.

    Returns the number of properties added. Extractors report a missing value as
    None; those are dropped rather than written as null properties.
    """
    assets = item.get("assets") or {}
    if not assets:
        return 0
    # Assets of an item are all derived from the same source granule, so the
    # first one is representative for metadata purposes.
    href = next(iter(assets.values()))["href"]

    properties = item.setdefault("properties", {})
    added = 0
    for name in extractors:
        for key, value in EXTRACTORS[name](href).items():
            if value is not None:
                properties[key] = value
                added += 1
    return added


@task
def enrich_disaster_metadata(discovery_result: dict, extractors: list = None, dag_run=None) -> dict:
    """Annotate discovered items with disasters metadata.

    Args:
        discovery_result: return value of `discover_from_s3_task` - the discovery
            event plus a `payload` list of S3 keys holding the discovered objects.
        extractors: names of extractors to apply, from `EXTRACTORS`. Falls back to
            `disasters_extractors` in the DAG run config, so the task works for
            both scheduled runs (event) and manual runs (conf).

    Returns:
        `discovery_result` unchanged, so downstream tasks consume it as they would
        consume discovery output directly.
    """
    if extractors is None:
        extractors = (dag_run.conf or {}).get("disasters_extractors", []) if dag_run else []

    if unknown := [name for name in extractors if name not in EXTRACTORS]:
        raise ValueError(
            f"Unknown disasters extractor(s): {unknown}. "
            f"Valid extractors are: {sorted(EXTRACTORS)}"
        )

    # Discovery short-circuits to {} when it finds no files.
    payload_keys = discovery_result.get("payload", []) if discovery_result else []
    if not extractors or not payload_keys:
        print(f"Nothing to enrich ({extractors=}, {len(payload_keys)} payload files)")
        return discovery_result

    items_seen = 0
    items_enriched = 0
    for payload_key in payload_keys:
        with smart_open.open(payload_key, "r") as file:
            payload = json.loads(file.read())

        for item in payload.get("objects", []):
            items_seen += 1
            if enrich_item(item, extractors):
                items_enriched += 1

        with smart_open.open(payload_key, "w") as file:
            file.write(json.dumps(payload))

    # A large gap between these two counts usually means the GeoTIFF tags could
    # not be read at all - worth surfacing, since extraction failures are
    # otherwise silent.
    print(
        f"Applied extractors {extractors} to {items_seen} items across "
        f"{len(payload_keys)} payload files; {items_enriched} gained properties"
    )
    return discovery_result
