"""Test integrity of dags."""

import importlib
import os
import pytest

from airflow.models import DagBag

@pytest.fixture()
def dag_bag():
    return DagBag(dag_folder="dags", include_examples=False)

def test_import_dags(dag_bag):
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