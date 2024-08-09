from dags.veda_data_pipeline.utils import submit_stac

import os
import boto3
import pytest
from moto import mock_secretsmanager
import requests_mock

@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-west-2"
    os.environ["COGNITO_APP_SECRET"] = "app_secret"
    os.environ["STAC_INGESTOR_API_URL"] = "http://www.test.com"

@pytest.fixture(scope="function")
def aws(aws_credentials):
    with mock_secretsmanager():
        yield boto3.client("secretsmanager", region_name="us-west-2")

@pytest.fixture
def create_secret(aws):
    boto3.client("secretsmanager", region_name="us-west-2").create_secret(Name="app_secret", SecretString="{\"cognito_domain\": \"http://test.com\", \"client_id\": \"test_id\", \"client_secret\": \"test_secret\", \"scope\": \"test_scope\"}")

@requests_mock.Mocker(kw="mock")
def test_submission_handler_dry_run(create_secret, capsys, **kwargs):
  token_endpoint = kwargs["mock"].post("http://test.com/oauth2/token", json={"token_type": "bearer", "access_token": "token"})
  ingestions_endpoint = kwargs["mock"].post("http://www.test.com/ingestions", json={"token_type": "bearer", "access_token": "token"})
  fake_event = {
    "dry_run": "dry run",
    "stac_file_url": "http://www.test.com",
    "stac_item": 123
  }

  res = submit_stac.submission_handler(fake_event)

  assert res == None
  captured = capsys.readouterr()
  assert "Dry run, not inserting" in captured.out
  assert token_endpoint.call_count == 0
  assert ingestions_endpoint.call_count == 0

@requests_mock.Mocker(kw="mock")
def test_submission_handler( create_secret, capsys, **kwargs):
  token_endpoint = kwargs["mock"].post("http://test.com/oauth2/token", json={"token_type": "bearer", "access_token": "token"})
  ingestions_endpoint = kwargs["mock"].post("http://www.test.com/ingestions", json={"token_type": "bearer", "access_token": "token"})
  fake_event = {
    "stac_file_url": "http://www.test.com",
    "stac_item": 123
  }

  res = submit_stac.submission_handler(fake_event)

  assert res == None
  captured = capsys.readouterr()
  assert "Dry run, not inserting" not in captured.out
  assert token_endpoint.call_count == 1
  assert ingestions_endpoint.call_count == 1