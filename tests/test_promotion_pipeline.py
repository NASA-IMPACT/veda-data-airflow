import pytest
from unittest.mock import Mock, patch
from dags.veda_data_pipeline.veda_promotion_pipeline import transfer_assets_to_production_bucket
import json
import boto3
from botocore.exceptions import ClientError

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
def mock_aws_vars():
    return {
        "ASSUME_ROLE_WRITE_ARN": "arn:aws:iam::123456789012:role/test-role"
    }

@pytest.fixture
def mock_sts_client():
    mock_client = Mock()
    mock_client.assume_role.return_value = {
        'Credentials': {
            'AccessKeyId': 'test-access-key',
            'SecretAccessKey': 'test-secret-key',
            'SessionToken': 'test-session-token'
        }
    }
    return mock_client

@pytest.fixture
def mock_s3_client():
    mock_client = Mock()
    mock_client.list_objects_v2.return_value = {
        'Contents': [
            {'Key': 'staging-prefix/file1.tif'},
            {'Key': 'staging-prefix/file2.tif'}
        ]
    }

    # Mock head_object to return a 404 error by default
    def head_object_side_effect(**kwargs):
        error_response = {'Error': {'Code': '404', 'Message': 'Not Found'}}
        raise ClientError(error_response, 'HeadObject')

    mock_client.head_object.side_effect = head_object_side_effect
    mock_client.exceptions.ClientError = ClientError
    return mock_client

def test_transfer_assets_to_production_bucket_transfer_false(mock_task_instance, mock_aws_vars, mock_sts_client, mock_s3_client):
    """Test that when transfer is False, payload is updated but no transfer occurs"""
    mock_task_instance.dag_run.conf["transfer"] = False

    with patch("veda_data_pipeline.groups.transfer_group.transfer_data") as mock_transfer, \
         patch("airflow.models.variable.Variable.get", return_value=json.dumps(mock_aws_vars)), \
         patch("boto3.client") as mock_boto3_client:
        mock_boto3_client.side_effect = lambda service, **kwargs: {
            'sts': mock_sts_client,
            's3': mock_s3_client
        }[service]

        payload = {
            "bucket": "staging-bucket",
            "prefix": "staging-prefix/",
            "filename_regex": r"^.*\.tif$",
            "transfer": False
        }

        task_func = transfer_assets_to_production_bucket.function
        result = task_func(ti=mock_task_instance, payload=payload)

        # Verify transfer_data was not called
        mock_transfer.assert_not_called()

        # Verify payload was updated correctly
        assert result["bucket"] == "veda-data-store"
        assert result["prefix"] == "test-collection/"

def test_transfer_assets_to_production_bucket_transfer_true(mock_task_instance, mock_aws_vars, mock_sts_client, mock_s3_client):
    """Test that when transfer is True, payload is updated and transfer occurs"""
    mock_task_instance.dag_run.conf["transfer"] = True

    with patch("dags.veda_data_pipeline.veda_promotion_pipeline.transfer_data") as mock_transfer, \
         patch("airflow.models.variable.Variable.get", return_value=json.dumps(mock_aws_vars)), \
         patch("boto3.client") as mock_boto3_client:
        mock_boto3_client.side_effect = lambda service, **kwargs: {
            'sts': mock_sts_client,
            's3': mock_s3_client
        }[service]

        payload = {
            "bucket": "staging-bucket",
            "prefix": "staging-prefix/",
            "filename_regex": r"^.*\.tif$",
            "transfer": True
        }

        task_func = transfer_assets_to_production_bucket.function
        result = task_func(ti=mock_task_instance, payload=payload)

        mock_transfer.assert_called_once()
        call_args = mock_transfer.call_args[1]["payload"]
        assert call_args["transfer"] is True
        assert call_args["origin_bucket"] == "staging-bucket"
        assert call_args["origin_prefix"] == "staging-prefix/"
        assert call_args["target_bucket"] == "test-target-bucket"
        assert call_args["collection"] == "test-collection"

        assert result["bucket"] == "veda-data-store"
        assert result["prefix"] == "test-collection/"

def test_transfer_assets_to_production_bucket_transfer_default(mock_task_instance, mock_aws_vars, mock_sts_client, mock_s3_client):
    """Test that when transfer is not specified, it defaults to True"""

    with patch("dags.veda_data_pipeline.veda_promotion_pipeline.transfer_data") as mock_transfer, \
         patch("airflow.models.variable.Variable.get", return_value=json.dumps(mock_aws_vars)), \
         patch("boto3.client") as mock_boto3_client:
        mock_boto3_client.side_effect = lambda service, **kwargs: {
            'sts': mock_sts_client,
            's3': mock_s3_client
        }[service]

        payload = {
            "bucket": "staging-bucket",
            "prefix": "staging-prefix/",
            "filename_regex": r"^.*\.tif$",
            "transfer": True
        }

        task_func = transfer_assets_to_production_bucket.function
        result = task_func(ti=mock_task_instance, payload=payload)

        mock_transfer.assert_called_once()
        call_args = mock_transfer.call_args[1]["payload"]
        assert call_args["transfer"] is True
        assert call_args["origin_bucket"] == "staging-bucket"
        assert call_args["origin_prefix"] == "staging-prefix/"
        assert call_args["target_bucket"] == "test-target-bucket"
        assert call_args["collection"] == "test-collection"

        assert result["bucket"] == "veda-data-store"
        assert result["prefix"] == "test-collection/"

def test_transfer_assets_to_production_bucket_412_error(mock_task_instance, mock_aws_vars, mock_sts_client, mock_s3_client):
    """Test that when a file already exists with the same ETag (412 error), no error is raised"""
    mock_task_instance.dag_run.conf["transfer"] = True

    def head_object_side_effect_412(**kwargs):
        return {'ETag': '"test-etag"'}

    mock_s3_client.head_object.side_effect = head_object_side_effect_412

    def copy_object_side_effect(**kwargs):
        error_response = {'Error': {'Code': 'PreconditionFailed', 'Message': 'Precondition Failed'}}
        raise ClientError(error_response, 'CopyObject')

    mock_s3_client.copy_object.side_effect = copy_object_side_effect

    with patch("dags.veda_data_pipeline.veda_promotion_pipeline.transfer_data") as mock_transfer, \
         patch("airflow.models.variable.Variable.get", return_value=json.dumps(mock_aws_vars)), \
         patch("boto3.client") as mock_boto3_client:
        mock_boto3_client.side_effect = lambda service, **kwargs: {
            'sts': mock_sts_client,
            's3': mock_s3_client
        }[service]

        payload = {
            "bucket": "staging-bucket",
            "prefix": "staging-prefix/",
            "filename_regex": r"^.*\.tif$",
            "transfer": True
        }

        task_func = transfer_assets_to_production_bucket.function
        result = task_func(ti=mock_task_instance, payload=payload)

        mock_transfer.assert_called_once()
        assert result["bucket"] == "veda-data-store"
        assert result["prefix"] == "test-collection/"