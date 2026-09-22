"""The configuration task skips without stopping CloudFront invalidation.

Under Airflow's default `all_success` rule a skipped task skips everything downstream, so
a skipped `configure_table` would suppress the invalidation too. `invalidate_cloudfront`
therefore runs under `all_done`: the ingest changed the data whether the configuration
succeeded, skipped or failed, so the cache is stale in every case.

`all_done` alone would hide a failure. A DAG run takes its state from the leaf tasks, and
`End` is the only leaf, so an `End` reached solely through a successful invalidation would
report success even when the configuration failed. `End` runs under `none_failed` and
depends on the configuration task directly, which puts the failure on a leaf.
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


def test_cloudfront_runs_whatever_the_configuration_did(dag):
    assert dag.get_task("invalidate_cloudfront").trigger_rule == TriggerRule.ALL_DONE


def test_end_surfaces_a_failed_configuration(dag):
    """Without both of these, all_done lets a failed configuration report a green run."""
    end = dag.get_task("End")
    assert end.trigger_rule == TriggerRule.NONE_FAILED
    assert "configure_table" in end.upstream_task_ids


def test_end_is_the_only_leaf(dag):
    """The DAG run's state comes from the leaves, so this is what the rules above hinge on."""
    leaves = {t.task_id for t in dag.tasks if not t.downstream_list}
    assert leaves == {"End"}


def test_configure_table_keeps_the_default_rule(dag):
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
