"""Test integrity of dags."""
import importlib
import os
import pytest

from os.path import dirname

from airflow.models import DagBag
from airflow import models as af_models
from airflow.utils.dag_cycle_tester import check_cycle

DAG_PATH = os.path.join(
    dirname(dirname(__file__)), 'dags/veda_data_pipeline'
)

DAG_FILES = [f for f in os.listdir(DAG_PATH) if f.endswith('.py')]

@pytest.fixture()
def dag_bag():

    return DagBag(dag_folder="dags/veda_data_pipeline", include_examples=False)

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