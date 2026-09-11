import pytest

from veda_data_pipeline.utils.vector_ingest.table_config import (
    ALLOWED_INDEX_METHODS,
    MAX_IDENTIFIER_LENGTH,
    InvalidTableConfig,
    build_statements,
    index_name,
)


def test_no_config_produces_no_statements():
    """Omitting table_config is how a backfill's intermediate runs skip indexing."""
    assert build_statements("hms_smoke", None) == []
    assert build_statements("hms_smoke", {}) == []


def test_single_index_and_analyze():
    statements = build_statements(
        "hms_smoke", {"indexes": [{"columns": ["datetime"]}]}
    )
    assert statements == [
        'CREATE INDEX CONCURRENTLY IF NOT EXISTS "idx_hms_smoke_datetime" '
        'ON "public"."hms_smoke" USING btree ("datetime")',
        'ANALYZE "public"."hms_smoke"',
    ]


def test_concurrently_is_the_default():
    """A blocking build on a table the Features API serves is a visible outage."""
    (create, _) = build_statements("t", {"indexes": [{"columns": ["a"]}]})
    assert "CONCURRENTLY" in create


def test_concurrently_can_be_disabled():
    (create, _) = build_statements(
        "t", {"indexes": [{"columns": ["a"], "concurrently": False}]}
    )
    assert "CONCURRENTLY" not in create


def test_if_not_exists_so_reruns_are_no_ops():
    (create, _) = build_statements("t", {"indexes": [{"columns": ["a"]}]})
    assert "IF NOT EXISTS" in create


def test_multi_column_index():
    (create, _) = build_statements(
        "t", {"indexes": [{"columns": ["datetime", "density_rank"]}]}
    )
    assert '("datetime", "density_rank")' in create
    assert '"idx_t_datetime_density_rank"' in create


def test_schema_defaults_to_public_and_is_honoured():
    (create, _) = build_statements(
        "t", {"schema": "vector", "indexes": [{"columns": ["a"]}]}
    )
    assert 'ON "vector"."t"' in create


def test_analyze_can_be_disabled():
    statements = build_statements(
        "t", {"indexes": [{"columns": ["a"]}], "analyze": False}
    )
    assert len(statements) == 1
    assert "ANALYZE" not in statements[0]


def test_analyze_only_config_is_valid():
    assert build_statements("t", {"analyze": True}) == ['ANALYZE "public"."t"']


def test_explicit_index_name_is_used():
    (create, _) = build_statements(
        "t", {"indexes": [{"columns": ["a"], "name": "my_index"}]}
    )
    assert '"my_index"' in create


@pytest.mark.parametrize("method", sorted(ALLOWED_INDEX_METHODS))
def test_allowed_methods(method):
    (create, _) = build_statements(
        "t", {"indexes": [{"columns": ["geom"], "method": method}]}
    )
    assert f"USING {method} " in create


def test_unsupported_method_is_rejected():
    """`method` reaches SQL as a bare keyword, so it must be allowlisted."""
    with pytest.raises(InvalidTableConfig, match="unsupported index method"):
        build_statements("t", {"indexes": [{"columns": ["a"], "method": "btree; DROP TABLE x"}]})


@pytest.mark.parametrize(
    "bad",
    ['a"; DROP TABLE users; --', "a b", "a-b", "1abc", "", "a;b", "a)"],
)
def test_malicious_or_odd_identifiers_are_rejected(bad):
    with pytest.raises(InvalidTableConfig):
        build_statements("t", {"indexes": [{"columns": [bad]}]})


def test_bad_collection_name_is_rejected():
    with pytest.raises(InvalidTableConfig, match="invalid collection"):
        build_statements('t"; DROP TABLE x; --', {"indexes": [{"columns": ["a"]}]})


def test_bad_schema_is_rejected():
    with pytest.raises(InvalidTableConfig, match="invalid schema"):
        build_statements("t", {"schema": "public; DROP TABLE x", "indexes": []})


def test_index_without_columns_is_rejected():
    with pytest.raises(InvalidTableConfig, match="no columns"):
        build_statements("t", {"indexes": [{"method": "btree"}]})


def test_index_name_is_truncated_to_postgres_limit():
    """An over-long name would be truncated by Postgres, so IF NOT EXISTS would miss it
    and the index would be rebuilt on every run."""
    name = index_name("t" * 50, ["c" * 50], "btree")
    assert len(name) == MAX_IDENTIFIER_LENGTH


def test_index_name_is_deterministic():
    assert index_name("t", ["a", "b"], "btree") == index_name("t", ["a", "b"], "btree")


def test_non_btree_method_is_part_of_the_name():
    """Two indexes on the same column with different methods must not collide."""
    assert index_name("t", ["geom"], "btree") != index_name("t", ["geom"], "gist")


def test_several_indexes_are_all_emitted():
    statements = build_statements(
        "t",
        {
            "indexes": [
                {"columns": ["datetime"]},
                {"columns": ["geom"], "method": "gist"},
            ]
        },
    )
    assert len(statements) == 3  # two creates plus ANALYZE
    assert "USING btree" in statements[0]
    assert "USING gist" in statements[1]
