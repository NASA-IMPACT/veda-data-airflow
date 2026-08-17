"""Test integrity of dags."""

import importlib
from pathlib import Path

import pytest
from airflow.models import DagBag
from airflow.sdk import DAG
from airflow.utils.dag_cycle_tester import check_cycle

DAG_PATH = Path(__file__).resolve().parents[1] / "dags" / "veda_data_pipeline"

DAG_FILES = [path.name for path in DAG_PATH.iterdir() if path.suffix == ".py"]


@pytest.fixture()
def dag_bag():
    return DagBag(dag_folder=str(DAG_PATH), include_examples=False)


@pytest.mark.parametrize("dag_file", DAG_FILES)
def test_dag_integrity(dag_file):
    """Import dag files and check for DAG."""
    module_name = Path(dag_file).stem
    module_path = DAG_PATH / dag_file
    mod_spec = importlib.util.spec_from_file_location(module_name, module_path)
    module = importlib.util.module_from_spec(mod_spec)
    mod_spec.loader.exec_module(module)

    dag_objects = [var for var in vars(module).values() if isinstance(var, DAG)]

    for dag in dag_objects:
        check_cycle(dag)
