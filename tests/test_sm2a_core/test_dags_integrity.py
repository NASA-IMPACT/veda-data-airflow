from airflow.models import DagBag
from slack_notifications import slack_fail_alert


def get_dag_bag():
    return DagBag(dag_folder="dags", include_examples=False)


def test_import_dags():
    """
    Test all the libraries can be imported
    """
    dag_bag = get_dag_bag()
    for dag_id, dag in dag_bag.dags.items():
        print(f"{dag_id}: {dag.fileloc}")
    assert not dag_bag.import_errors, f"DAG has an import error {dag_bag.import_errors}"


def test_dags_exist():
    """
    Test if at least one DAG exists
    """
    dag_bag = get_dag_bag()
    assert len(dag_bag.dags) > 0, "No DAGs found in the dag folder."


def test_dags_have_failure_callback():
    """
    Test that all DAGs have on_failure_callback set to slack_fail_alert
    """
    dag_bag = get_dag_bag()
    for dag_id, dag in dag_bag.dags.items():
        assert hasattr(dag, "on_failure_callback"), f"DAG {dag_id} is missing on_failure_callback"
        assert dag.on_failure_callback == slack_fail_alert, (
            f"DAG {dag_id} does not have slack_fail_alert set as on_failure_callback"
        )
