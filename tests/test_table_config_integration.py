"""Integration tests for table_config against a real Postgres.

Skipped unless TEST_DATABASE_URL is set, so the default test run stays offline.

    docker run -d --name veda-test-pg -p 55432:5432 \
        -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=postgres \
        postgis/postgis:16-3.4

    TEST_DATABASE_URL=postgresql://postgres:postgres@localhost:55432/postgres \
        pytest tests/test_table_config_integration.py -v

These cover what the unit tests cannot: that the generated DDL is valid Postgres, that
CREATE INDEX CONCURRENTLY works under the autocommit handling, and that the invalid-index
query returns what it claims to.
"""

import os

import pytest

from veda_data_pipeline.utils.vector_ingest.table_config import (
    find_invalid_indexes,
    run_statements,
)

DATABASE_URL = os.getenv("TEST_DATABASE_URL")

pytestmark = pytest.mark.skipif(
    not DATABASE_URL, reason="set TEST_DATABASE_URL to run integration tests"
)

TABLE = "hms_smoke_test"


@pytest.fixture
def conn():
    import psycopg2

    connection = psycopg2.connect(DATABASE_URL)
    connection.autocommit = True
    with connection.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS public.{TABLE}")
        cur.execute(
            f"""
            CREATE TABLE public.{TABLE} (
                fid           serial PRIMARY KEY,
                datetime      timestamptz,
                density       text,
                density_rank  integer,
                geom          geometry(MultiPolygon, 4326)
            )
            """
        )
        cur.execute(
            f"INSERT INTO public.{TABLE} (datetime, density, density_rank) "
            f"SELECT now(), 'Light', 1 FROM generate_series(1, 500)"
        )
    yield connection
    with connection.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS public.{TABLE}")
    connection.close()


def indexes_on(conn, table=TABLE):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT indexname FROM pg_indexes WHERE schemaname='public' AND tablename=%s",
            (table,),
        )
        return {row[0] for row in cur.fetchall()}


def test_creates_the_index_concurrently(conn):
    """The default path: CONCURRENTLY, which needs autocommit to work at all."""
    result = run_statements(conn, TABLE, {"indexes": [{"columns": ["datetime"]}]})
    assert result["status"] == "success"
    assert f"idx_{TABLE}_datetime" in indexes_on(conn)


def test_rerun_is_a_no_op(conn):
    """IF NOT EXISTS plus deterministic naming means re-ingest does not rebuild."""
    config = {"indexes": [{"columns": ["datetime"]}]}
    run_statements(conn, TABLE, config)
    before = indexes_on(conn)
    run_statements(conn, TABLE, config)
    assert indexes_on(conn) == before


def test_non_concurrent_index(conn):
    run_statements(
        conn, TABLE, {"indexes": [{"columns": ["density"], "concurrently": False}]}
    )
    assert f"idx_{TABLE}_density" in indexes_on(conn)


def test_multi_column_index(conn):
    run_statements(conn, TABLE, {"indexes": [{"columns": ["datetime", "density_rank"]}]})
    assert f"idx_{TABLE}_datetime_density_rank" in indexes_on(conn)


def test_gist_index_on_geometry(conn):
    run_statements(conn, TABLE, {"indexes": [{"columns": ["geom"], "method": "gist"}]})
    assert f"idx_{TABLE}_geom_gist" in indexes_on(conn)


def test_analyze_populates_planner_statistics(conn):
    """Without stats the planner will not use the index that was just built."""
    with conn.cursor() as cur:
        cur.execute(f"DELETE FROM pg_statistic WHERE starelid = 'public.{TABLE}'::regclass")
    run_statements(conn, TABLE, {"indexes": [{"columns": ["datetime"]}], "analyze": True})
    with conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM pg_stats WHERE schemaname='public' AND tablename=%s",
            (TABLE,),
        )
        assert cur.fetchone()[0] > 0


def test_several_indexes_in_one_config(conn):
    run_statements(
        conn,
        TABLE,
        {
            "indexes": [
                {"columns": ["datetime"]},
                {"columns": ["density_rank"]},
                {"columns": ["geom"], "method": "gist"},
            ]
        },
    )
    created = indexes_on(conn)
    assert {
        f"idx_{TABLE}_datetime",
        f"idx_{TABLE}_density_rank",
        f"idx_{TABLE}_geom_gist",
    } <= created


def test_no_invalid_indexes_after_a_clean_run(conn):
    run_statements(conn, TABLE, {"indexes": [{"columns": ["datetime"]}]})
    assert find_invalid_indexes(conn.cursor(), TABLE) == []


def test_invalid_index_is_detected(conn):
    """An interrupted CONCURRENTLY build leaves an INVALID index that IF NOT EXISTS
    considers present, so it would never be repaired. Simulated by marking it invalid."""
    run_statements(conn, TABLE, {"indexes": [{"columns": ["datetime"]}]})
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE pg_index SET indisvalid = false "
            f"WHERE indexrelid = 'public.idx_{TABLE}_datetime'::regclass"
        )
    assert find_invalid_indexes(conn.cursor(), TABLE) == [f"idx_{TABLE}_datetime"]


def test_empty_config_touches_nothing(conn):
    before = indexes_on(conn)
    assert run_statements(conn, TABLE, None)["status"] == "skipped"
    assert run_statements(conn, TABLE, {})["status"] == "skipped"
    assert indexes_on(conn) == before
