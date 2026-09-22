"""The configuration task skips without stopping CloudFront invalidation.

Under Airflow's default `all_success` rule a skipped task skips everything downstream, so
a skipped `configure_table` would suppress the invalidation too. `invalidate_cloudfront`
therefore runs under `none_failed`: it survives a skip, but is still withheld when the
table configuration fails.
"""

import pytest
from airflow.exceptions import AirflowSkipException
from airflow.utils.trigger_rule import TriggerRule

from veda_data_pipeline.veda_vector_pipeline import configure_table, get_ingest_vector_dag


class FakeDagRun:
    def __init__(self, conf):
        self.conf = conf


def run_configure_table(conf):
    """Call the task's underlying function directly, outside an Airflow run."""
    return configure_table.function(dag_run=FakeDagRun(conf))


@pytest.fixture(scope="module")
def dag():
    return get_ingest_vector_dag(id="test_ingest_vector_skip", event={})


def test_cloudfront_runs_after_a_skip(dag):
    assert dag.get_task("invalidate_cloudfront").trigger_rule == TriggerRule.NONE_FAILED


def test_other_tasks_keep_their_defaults(dag):
    assert dag.get_task("configure_table").trigger_rule == TriggerRule.ALL_SUCCESS


def test_skips_without_table_config():
    with pytest.raises(AirflowSkipException, match="No table_config provided"):
        run_configure_table({"collection": "hms_smoke"})


def test_skips_when_table_config_is_empty():
    with pytest.raises(AirflowSkipException):
        run_configure_table({"collection": "hms_smoke", "table_config": {}})


def test_skips_without_an_explicit_collection():
    """A per-file `id_template` names a table per file, so there is nothing to configure."""
    with pytest.raises(AirflowSkipException, match="explicit `collection`"):
        run_configure_table(
            {"collection": "", "table_config": {"indexes": [{"columns": ["datetime"]}]}}
        )
