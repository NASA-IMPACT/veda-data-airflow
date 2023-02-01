import os
from unittest import mock

import pytest
import boto3
from moto import mock_s3

from mypy_boto3_s3.service_resource import S3ServiceResource, Bucket


@pytest.fixture(scope="session", autouse=True)
def mock_environment():
    with mock.patch.dict(os.environ, {"BUCKET": "test-bucket"}):
        yield os.environ


@pytest.fixture
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"


@pytest.fixture
def s3_client(aws_credentials):
    with mock_s3():
        yield boto3.client("s3", region_name="us-east-1")


@pytest.fixture
def s3_resource(aws_credentials) -> S3ServiceResource:
    with mock_s3():
        yield boto3.resource("s3", region_name="us-east-1")


@pytest.fixture
def s3_created_bucket(s3_resource, mock_environment) -> Bucket:
    s3_bucket = s3_resource.Bucket(mock_environment["BUCKET"])

    s3_bucket.create()
    yield s3_bucket
