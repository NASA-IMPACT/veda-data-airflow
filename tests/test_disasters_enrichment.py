import json
import os

import boto3
import pytest
import smart_open
from moto import mock_aws

from veda_data_pipeline.groups import disasters_group
from veda_data_pipeline.groups.disasters_group import enrich_disaster_metadata, enrich_item


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials, to ensure we're not touching AWS directly"""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["EVENT_BUCKET"] = "test"


def make_item(href, **properties):
    return {
        "collection": "disasters-test",
        "item_id": "item-1",
        "assets": {"cog_default": {"href": href}},
        "properties": properties,
    }


# `product` is the one extractor that reads only the S3 path, so it exercises the
# real code path without needing a GeoTIFF on disk.
S3_HREF = "s3://test/ProgramData/Sentinel-2/TrueColor/scene_20240115.tif"


def test_enrich_item_adds_extracted_properties():
    item = make_item(S3_HREF, existing="kept")

    added = enrich_item(item, ["product"])

    assert added == 2
    assert item["properties"] == {
        "existing": "kept",
        "sensor": "Sentinel-2",
        "product": "TrueColor",
    }


def test_enrich_item_drops_none_values(monkeypatch):
    """Extractors report a missing value as None; those must not become null properties."""
    monkeypatch.setitem(
        disasters_group.EXTRACTORS,
        "product",
        lambda href: {"sensor": "Sentinel-2", "product": None},
    )
    item = make_item(S3_HREF)

    added = enrich_item(item, ["product"])

    assert added == 1
    assert item["properties"] == {"sensor": "Sentinel-2"}


def test_enrich_item_without_assets_is_skipped():
    item = {"item_id": "item-1", "assets": {}}

    assert enrich_item(item, ["product"]) == 0


def test_enrich_item_creates_missing_properties_key():
    item = {"item_id": "item-1", "assets": {"cog_default": {"href": S3_HREF}}}

    enrich_item(item, ["product"])

    assert item["properties"]["sensor"] == "Sentinel-2"


@mock_aws
def test_enrich_disaster_metadata_rewrites_payload_files(aws_credentials):
    s3 = boto3.resource("s3")
    bucket = s3.Bucket("test")
    bucket.create(CreateBucketConfiguration={"LocationConstraint": "us-west-2"})

    payload_key = "s3://test/events/disasters-test/s3_discover_output_1.json"
    with smart_open.open(payload_key, "w") as file:
        file.write(json.dumps({"objects": [make_item(S3_HREF)]}))

    discovery_result = {"collection": "disasters-test", "payload": [payload_key], "discovered": [1]}

    result = enrich_disaster_metadata.function(discovery_result, extractors=["product"])

    # The task hands discovery output straight through to get_files_task.
    assert result == discovery_result

    with smart_open.open(payload_key, "r") as file:
        enriched = json.loads(file.read())
    assert enriched["objects"][0]["properties"] == {
        "sensor": "Sentinel-2",
        "product": "TrueColor",
    }


@mock_aws
def test_enrich_disaster_metadata_no_extractors_leaves_payload_untouched(aws_credentials):
    s3 = boto3.resource("s3")
    bucket = s3.Bucket("test")
    bucket.create(CreateBucketConfiguration={"LocationConstraint": "us-west-2"})

    payload_key = "s3://test/events/disasters-test/s3_discover_output_1.json"
    original = {"objects": [make_item(S3_HREF)]}
    with smart_open.open(payload_key, "w") as file:
        file.write(json.dumps(original))

    discovery_result = {"payload": [payload_key]}

    assert enrich_disaster_metadata.function(discovery_result, extractors=[]) == discovery_result

    with smart_open.open(payload_key, "r") as file:
        assert json.loads(file.read()) == original


def test_enrich_disaster_metadata_handles_empty_discovery():
    """discover_from_s3_task returns {} when it finds no files."""
    assert enrich_disaster_metadata.function({}, extractors=["product"]) == {}


def test_enrich_disaster_metadata_rejects_unknown_extractor():
    with pytest.raises(ValueError, match="Unknown disasters extractor"):
        enrich_disaster_metadata.function({"payload": []}, extractors=["not_an_extractor"])


def test_disasters_discover_dag_inserts_enrichment_after_discovery():
    from veda_data_pipeline.veda_disasters_discover_pipeline import get_disasters_discover_dag

    dag = get_disasters_discover_dag(id="veda_disasters_discover_test", event={})

    upstream = {t.task_id: sorted(t.upstream_task_ids) for t in dag.tasks}
    assert upstream["enrich_disaster_metadata"] == ["discover_from_s3_task"]
    # Enrichment sits between discovery and the rest of the standard pipeline.
    assert upstream["get_files_task"] == ["enrich_disaster_metadata"]
