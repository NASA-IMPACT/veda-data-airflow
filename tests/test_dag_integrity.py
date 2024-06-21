"""Test integrity of dags."""
import boto3
import importlib
import os
import pytest

from os.path import dirname

from airflow.models import DagBag
from airflow import models as af_models
from airflow.utils.dag_cycle_tester import check_cycle
from moto import mock_s3


@pytest.fixture(scope='function')
def aws_credentials():
    """Mocked AWS Credentials, to ensure we're not touching AWS directly"""
    os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
    os.environ['AWS_SECURITY_TOKEN'] = 'testing'
    os.environ['AWS_SESSION_TOKEN'] = 'testing'
    os.environ['EVENT_BUCKET'] = 'test'

DAG_PATH = os.path.join(
    dirname(dirname(__file__)), 'dags/veda_data_pipeline'
)

DAG_FILES = [f for f in os.listdir(DAG_PATH) if f.endswith('.py')]

@pytest.fixture()
@mock_s3
def dag_bag(aws_credentials):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket("test")
    # Create the bucket first, as we're interacting with an empty mocked 'AWS account'
    bucket.create(
      CreateBucketConfiguration={'LocationConstraint': 'us-west-2'}
    ) 
    return DagBag(dag_folder="dags", include_examples=False)

def test_import_dags(dag_bag, ):
    """
    Test all the libraries can be imported
    """
    assert len(dag_bag.import_errors) == 0, f"DAG has an import Error {dag_bag.import_errors}"

def test_dags_exists(dag_bag):
    """
    Test if at least one DAG exists
    """
    dags = list()
    for dag_id, _ in dag_bag.dags.items():
        dags.append(dag_id)

    assert len(dags) > 0

@pytest.mark.parametrize('dag_file', DAG_FILES)
def test_dag_integrity(dag_file):
    """Import dag files and check for DAG."""
    module_name, _ = os.path.splitext(dag_file)
    module_path = os.path.join(DAG_PATH, dag_file)
    mod_spec = importlib.util.spec_from_file_location(module_name, module_path)
    module = importlib.util.module_from_spec(mod_spec)
    mod_spec.loader.exec_module(module)

    dag_objects = [var for var in vars(module).values() if isinstance(var, af_models.DAG)]

    for dag in dag_objects:
        check_cycle(dag)