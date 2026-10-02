import logging
from types import SimpleNamespace
from unittest import mock

import pytest
import requests

from veda_data_pipeline import veda_vector_pipeline
from veda_data_pipeline.veda_vector_pipeline import refresh_features_catalog

API_URL = "https://features.example.com/api/features"
REFRESH_URL = f"{API_URL}/refresh"

refresh = refresh_features_catalog.function


def dag_run(**conf):
    return SimpleNamespace(conf=conf, run_id="manual__2026-09-30T00:00:00+00:00")


def task_instance(try_number, max_tries=3):
    """Airflow retries while try_number <= max_tries (max_tries grows when cleared)."""
    return SimpleNamespace(try_number=try_number, max_tries=max_tries)


@pytest.fixture
def features_api_url():
    """Patch Variable.get so FEATURES_API_URL returns the given value."""
    with mock.patch.object(veda_vector_pipeline.Variable, "get") as get:

        def set_url(value):
            get.side_effect = lambda key, default=None: (
                value if key == "FEATURES_API_URL" else default
            )

        yield set_url


def test_variable_unset_skips(features_api_url, requests_mock, caplog):
    """No FEATURES_API_URL means the refresh is optional and nothing is called."""
    features_api_url(None)
    with caplog.at_level(logging.INFO):
        assert refresh(dag_run=dag_run(), ti=task_instance(1)) is None
    assert not requests_mock.called
    assert "Skipping features catalog refresh" in caplog.text


def test_conf_can_skip_the_refresh(features_api_url, requests_mock, caplog):
    """`refresh_catalog: false` lets a backfill's intermediate runs skip it."""
    features_api_url(API_URL)
    with caplog.at_level(logging.INFO):
        refresh(dag_run=dag_run(refresh_catalog=False), ti=task_instance(1))
    assert not requests_mock.called
    assert "Skipping features catalog refresh" in caplog.text


def test_success_calls_refresh_with_run_id(features_api_url, requests_mock):
    features_api_url(API_URL + "/")
    requests_mock.get(REFRESH_URL, json={"collections": 2})

    assert refresh(dag_run=dag_run(), ti=task_instance(1)) is None

    assert requests_mock.call_count == 1
    request = requests_mock.last_request
    assert request.url.split("?")[0] == REFRESH_URL
    assert request.qs == {"t": ["manual__2026-09-30t00:00:00+00:00"]}
    assert request.timeout == 60


def test_server_error_raises_while_retries_remain(features_api_url, requests_mock):
    """Raising is what makes Airflow retry."""
    features_api_url(API_URL)
    requests_mock.get(REFRESH_URL, status_code=500)

    with pytest.raises(requests.HTTPError):
        refresh(dag_run=dag_run(), ti=task_instance(1))


def test_server_error_on_last_try_logs_and_returns(features_api_url, requests_mock, caplog):
    """The data is already loaded; a failed refresh must not fail the run."""
    features_api_url(API_URL)
    requests_mock.get(REFRESH_URL, status_code=500)

    with caplog.at_level(logging.ERROR):
        assert refresh(dag_run=dag_run(), ti=task_instance(4)) is None
    assert "Features catalog refresh failed" in caplog.text


def test_cleared_task_still_retries(features_api_url, requests_mock):
    """After a clear, try_number keeps counting and max_tries moves up with it."""
    features_api_url(API_URL)
    requests_mock.get(REFRESH_URL, status_code=500)

    with pytest.raises(requests.HTTPError):
        refresh(dag_run=dag_run(), ti=task_instance(5, max_tries=7))


def test_connection_error_on_last_try_logs_and_returns(features_api_url, requests_mock, caplog):
    features_api_url(API_URL)
    requests_mock.get(REFRESH_URL, exc=requests.ConnectionError("unreachable"))

    with caplog.at_level(logging.ERROR):
        assert refresh(dag_run=dag_run(), ti=task_instance(4)) is None
    assert "unreachable" in caplog.text


def test_refresh_runs_between_configure_table_and_cloudfront():
    dag = veda_vector_pipeline.get_ingest_vector_dag("test_vector_dag", {})
    task = dag.get_task("refresh_features_catalog")
    assert task.upstream_task_ids == {"configure_table"}
    assert task.downstream_task_ids == {"invalidate_cloudfront"}
    assert task.retries == 3
    assert task.retry_delay.total_seconds() == 30
