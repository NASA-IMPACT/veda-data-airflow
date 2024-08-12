from dags.veda_data_pipeline.utils import transfer

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
def test_transfer_dry_run(aws_credentials, capsys):
  s3 = boto3.resource('s3')
  bucket_source = s3.Bucket("test_source")
  bucket_target = s3.Bucket("test_target")

  # Create the bucket first, as we're interacting with an empty mocked 'AWS account'
  bucket_source.create(
      CreateBucketConfiguration={'LocationConstraint': 'us-west-2'}
  )
  bucket_target.create(
      CreateBucketConfiguration={'LocationConstraint': 'us-west-2'}
  )
  # Create some example files that are representative of what the S3 bucket would look like in production
  client = boto3.client('s3', region_name='us-west-2')
  client.put_object(Bucket="test_source", Key="files/file1.cog", Body="stuff")
  client.put_object(Bucket="test_source", Key="files/file2.tif", Body="stuff")
  client.put_object(Bucket="test_source", Key="files/file3.txt", Body="stuff")

  fake_event = {
    "dry_run": "dry run",
    "origin_bucket": "test_source",
    "origin_prefix": "files",
    "filename_regex": r"[\s\S]*",
    "target_bucket": "test_target"
  }

  transfer.data_transfer_handler(fake_event)

  captured = capsys.readouterr()
  assert "Would have copied 3 files" in captured.out


@mock_s3
def test_transfer(aws_credentials, capsys):
  s3 = boto3.resource('s3')
  bucket_source = s3.Bucket("test_source")
  bucket_target = s3.Bucket("test_target")

  # Create the bucket first, as we're interacting with an empty mocked 'AWS account'
  bucket_source.create(
      CreateBucketConfiguration={'LocationConstraint': 'us-west-2'}
  )
  bucket_target.create(
      CreateBucketConfiguration={'LocationConstraint': 'us-west-2'}
  )
  # Create some example files that are representative of what the S3 bucket would look like in production
  client = boto3.client('s3', region_name='us-west-2')
  client.put_object(Bucket="test_source", Key="files/file1.cog", Body="stuff")
  client.put_object(Bucket="test_source", Key="files/file2.tif", Body="stuff")
  client.put_object(Bucket="test_source", Key="files/file3.txt", Body="stuff")

  fake_event = {
    "origin_bucket": "test_source",
    "origin_prefix": "files",
    "filename_regex": r"^.*\.(cog|tif)$",
    "target_bucket": "test_target"
  }

  transfer.data_transfer_handler(fake_event)


  captured = capsys.readouterr()
  
  target_bucket_objects = sum(1 for _ in bucket_target.objects.all())
  
  assert target_bucket_objects == 2