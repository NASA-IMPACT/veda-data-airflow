import boto3
import os
import pytest

from moto import mock_aws
from unittest.mock import Mock, patch
from veda_data_pipeline.veda_promotion_pipeline import transfer_assets_to_production_bucket

@pytest.fixture
def mock_task_instance():
    ti = Mock()
    ti.dag_run.conf = {
        "collection": "test-collection",
        "origin_bucket": "test-origin-bucket",
        "origin_prefix": "test-prefix/",
        "target_bucket": "test-target-bucket",
        "dry_run": False,
        "filename_regex": r"^.*\.tif$"
    }
    return ti

@pytest.fixture
def mock_variable_get():
    """Mock Variable.get to return individual variable values."""
    def _get(key, **kwargs):
        variables = {
            "ASSUME_ROLE_WRITE_ARN": "arn:aws:iam::123456789012:role/test-role",
        }
        if key == "aws_dags_variables" and kwargs.get("deserialize_json"):
            return {}
        return variables.get(key, "")
    return _get

@pytest.fixture
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"

@pytest.fixture
def s3():
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        # Create test buckets
        s3.create_bucket(Bucket="test-origin-bucket")
        s3.create_bucket(Bucket="test-target-bucket")

        s3.put_object(
            Bucket="test-origin-bucket",
            Key="test-prefix/file1.tif",
            Body="test content"
        )
        s3.put_object(
            Bucket="test-origin-bucket",
            Key="test-prefix/file2.tif",
            Body="test content"
        )
        yield s3

def test_transfer_assets_to_production_bucket_transfer_false(mock_task_instance, mock_variable_get, s3):
    """Test that when transfer is False, payload is updated but no transfer occurs"""
    mock_task_instance.dag_run.conf["transfer"] = False

    with patch("airflow.models.variable.Variable.get", side_effect=mock_variable_get):
        payload = {
            "bucket": "test-origin-bucket",
            "prefix": "test-prefix/",
            "filename_regex": r"^.*\.tif$",
            "transfer": False
        }

        task_func = transfer_assets_to_production_bucket.function
        result = task_func(dag_run=mock_task_instance.dag_run, payload=payload)

        response = s3.list_objects_v2(Bucket="test-target-bucket")
        assert "Contents" not in response
        assert result["bucket"] == "test-origin-bucket"
        assert result["prefix"] == "test-prefix/"

def test_transfer_assets_to_production_bucket_transfer_true(mock_task_instance, mock_variable_get, s3):
    """Test that when transfer is True, payload is updated and transfer occurs"""
    mock_task_instance.dag_run.conf["transfer"] = True

    with patch("airflow.models.variable.Variable.get", side_effect=mock_variable_get):
        payload = {
            "bucket": "test-origin-bucket",
            "prefix": "test-prefix/",
            "filename_regex": r"^.*\.tif$",
            "transfer": True
        }

        task_func = transfer_assets_to_production_bucket.function
        result = task_func(dag_run=mock_task_instance.dag_run, payload=payload)

        response = s3.list_objects_v2(Bucket="test-target-bucket")
        assert len(response["Contents"]) == 2
        assert all(obj["Key"].startswith("test-collection/") for obj in response["Contents"])
        assert result["bucket"] == "veda-data-store"
        assert result["prefix"] == "test-collection/"

def test_transfer_assets_to_production_bucket_412_error(mock_task_instance, mock_variable_get, s3):
    """Test that when a file already exists with the same ETag (412 error), no error is raised"""
    mock_task_instance.dag_run.conf["transfer"] = True

    s3.copy_object(
        CopySource={"Bucket": "test-origin-bucket", "Key": "test-prefix/file1.tif"},
        Bucket="test-target-bucket",
        Key="test-collection/file1.tif"
    )

    with patch("airflow.models.variable.Variable.get", side_effect=mock_variable_get):
        payload = {
            "bucket": "test-origin-bucket",
            "prefix": "test-prefix/",
            "filename_regex": r"^.*\.tif$",
            "transfer": True
        }

        task_func = transfer_assets_to_production_bucket.function
        result = task_func(dag_run=mock_task_instance.dag_run, payload=payload)

        assert result["bucket"] == "veda-data-store"
        assert result["prefix"] == "test-collection/"
        response = s3.list_objects_v2(Bucket="test-target-bucket")
        assert len(response["Contents"]) == 2
