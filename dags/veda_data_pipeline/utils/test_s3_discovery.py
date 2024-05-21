import s3_discovery
import pytest
import os
import boto3
from moto import mock_s3

from unittest.mock import patch

@pytest.fixture(scope='function')
def aws_credentials():
    """Mocked AWS Credentials, to ensure we're not touching AWS directly"""
    os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
    os.environ['AWS_SECURITY_TOKEN'] = 'testing'
    os.environ['AWS_SESSION_TOKEN'] = 'testing'
    os.environ['EVENT_BUCKET'] = 'test'

@mock_s3
def test_s3_discovery_dry_run(aws_credentials, capsys):
  s3 = boto3.resource('s3')
  bucket = s3.Bucket("test")
  # Create the bucket first, as we're interacting with an empty mocked 'AWS account'
  bucket.create(
      CreateBucketConfiguration={'LocationConstraint': 'us-west-2'}
  )

  # Create some example files that are representative of what the S3 bucket would look like in production
  client = boto3.client('s3', region_name='us-west-2')
  client.put_object(Bucket="test", Key="file1.cog", Body="stuff")
  client.put_object(Bucket="test", Key="file2.tif", Body="stuff")

  fake_event = {
    "dry_run": "dry run",
    "bucket": "test",
    "filename_regex": r"[\s\S]*"
  }

  res = s3_discovery.s3_discovery_handler(fake_event)

  captured = capsys.readouterr()
  assert "Running discovery in dry run mode" in captured.out
  
  assert res["discovered"] == 2