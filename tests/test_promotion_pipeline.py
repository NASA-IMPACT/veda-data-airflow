import pytest
from unittest.mock import Mock, patch
from dags.veda_data_pipeline.veda_promotion_pipeline import transfer_assets_to_production_bucket

@pytest.fixture
def mock_task_instance():
    ti = Mock()
    ti.dag_run.conf = {
        "collection": "test-collection",
        "origin_bucket": "test-origin-bucket",
        "origin_prefix": "test-prefix/",
        "target_bucket": "test-target-bucket",
        "dry_run": False,
    }
    return ti

def test_transfer_assets_to_production_bucket_transfer_false(mock_task_instance):
    """Test that when transfer is False, payload is updated but no transfer occurs"""
    mock_task_instance.dag_run.conf["transfer"] = False

    with patch("dags.veda_data_pipeline.groups.transfer_group.transfer_data") as mock_transfer:
        payload = {
            "bucket": "staging-bucket",
            "prefix": "staging-prefix/",
        }

        result = transfer_assets_to_production_bucket(ti=mock_task_instance, payload=payload)

        # Verify transfer_data was not called
        mock_transfer.assert_not_called()

        # Verify payload was updated correctly
        assert result["bucket"] == "veda-data-store"
        assert result["prefix"] == "test-collection/"

def test_transfer_assets_to_production_bucket_transfer_true(mock_task_instance):
    """Test that when transfer is True, payload is updated and transfer occurs"""
    mock_task_instance.dag_run.conf["transfer"] = True

    with patch("dags.veda_data_pipeline.groups.transfer_group.transfer_data") as mock_transfer:
        payload = {
            "bucket": "staging-bucket",
            "prefix": "staging-prefix/",
        }

        result = transfer_assets_to_production_bucket(ti=mock_task_instance, payload=payload)

        mock_transfer.assert_called_once()
        call_args = mock_transfer.call_args[1]["payload"]
        assert call_args["transfer"] is True
        assert call_args["origin_bucket"] == "staging-bucket"
        assert call_args["origin_prefix"] == "staging-prefix/"
        assert call_args["target_bucket"] == "test-target-bucket"
        assert call_args["collection"] == "test-collection"

        assert result["bucket"] == "veda-data-store"
        assert result["prefix"] == "test-collection/"

def test_transfer_assets_to_production_bucket_transfer_default(mock_task_instance):
    """Test that when transfer is not specified, it defaults to False"""

    with patch("dags.veda_data_pipeline.groups.transfer_group.transfer_data") as mock_transfer:
        payload = {
            "bucket": "staging-bucket",
            "prefix": "staging-prefix/",
        }

        result = transfer_assets_to_production_bucket(ti=mock_task_instance, payload=payload)

        mock_transfer.assert_not_called()

        assert result["bucket"] == "veda-data-store"
        assert result["prefix"] == "test-collection/"