"""Apply post-ingest table configuration (indexes, statistics) to a vector collection.

Indexes are deliberately created *after* the data is loaded: building a B-tree in one
bulk sort is cheaper than maintaining it row by row during ingest. The task that calls
this therefore sits downstream of the mapped ingest tasks, so it runs once after every
chunk has landed rather than once per chunk.

For a backfill split across several DAG runs, omit ``table_config`` from the conf of the
intermediate runs and include it only on the last one -- otherwise the index exists while
the remaining chunks are still loading, which is the case this ordering exists to avoid.

``ogr2ogr`` already creates the GiST index on the geometry column, and has no option for
an index on any other column. This covers the rest.
"""

import re

# Postgres identifiers as produced by ogr2ogr: lower-case alphanumerics and underscores.
# Anything else is rejected rather than escaped, so a surprising name fails loudly
# instead of being quietly quoted into a statement.
IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# Index methods worth allowing here. `method` reaches SQL as a bare keyword, so it is
# checked against this set rather than quoted.
ALLOWED_INDEX_METHODS = frozenset({"btree", "gist", "brin", "gin", "hash", "spgist"})

MAX_IDENTIFIER_LENGTH = 63  # Postgres truncates beyond this, silently


class InvalidTableConfig(ValueError):
    """Raised when a table_config block cannot be turned into safe SQL."""


def _identifier(value: str, kind: str) -> str:
    """Validate a Postgres identifier and return it double-quoted."""
    if not isinstance(value, str) or not IDENTIFIER_RE.match(value):
        raise InvalidTableConfig(
            f"invalid {kind}: {value!r} -- expected letters, digits and underscores"
        )
    return f'"{value}"'


def index_name(table: str, columns: list, method: str) -> str:
    """Build a deterministic index name, so re-runs are genuine no-ops.

    Truncated to Postgres' identifier limit; without this a long table plus several
    columns silently produces a different name than `IF NOT EXISTS` looks for, and the
    index gets rebuilt on every run.
    """
    name = f"idx_{table}_{'_'.join(columns)}"
    if method != "btree":
        name = f"{name}_{method}"
    return name[:MAX_IDENTIFIER_LENGTH]


def build_statements(collection: str, table_config: dict) -> list:
    """Turn a table_config block into the SQL statements that apply it.

    Kept separate from execution so the generated SQL can be tested without a database.
    """
    if not table_config:
        return []

    schema = table_config.get("schema", "public")
    schema_sql = _identifier(schema, "schema")
    table_sql = _identifier(collection, "collection")
    qualified = f"{schema_sql}.{table_sql}"

    statements = []
    for index in table_config.get("indexes", []):
        columns = index.get("columns")
        if not columns:
            raise InvalidTableConfig(f"index entry has no columns: {index!r}")

        method = index.get("method", "btree").lower()
        if method not in ALLOWED_INDEX_METHODS:
            raise InvalidTableConfig(
                f"unsupported index method {method!r}; "
                f"expected one of {sorted(ALLOWED_INDEX_METHODS)}"
            )

        columns_sql = ", ".join(_identifier(column, "column") for column in columns)
        name = index.get("name") or index_name(collection, columns, method)

        # CONCURRENTLY avoids the ACCESS EXCLUSIVE lock that would otherwise block reads
        # for the whole build -- on a large table being served by the features API that
        # is a visible outage, not just a slow ingest.
        concurrently = "CONCURRENTLY " if index.get("concurrently", True) else ""

        statements.append(
            f"CREATE INDEX {concurrently}IF NOT EXISTS {_identifier(name, 'index name')} "
            f"ON {qualified} USING {method} ({columns_sql})"
        )

    if table_config.get("analyze", True):
        # Without statistics the planner will not use the indexes just created.
        statements.append(f"ANALYZE {qualified}")

    return statements


def find_invalid_indexes(cursor, collection: str, schema: str = "public") -> list:
    """Return indexes left INVALID by an interrupted CONCURRENTLY build.

    A failed `CREATE INDEX CONCURRENTLY` leaves an index behind that is never used by the
    planner but is still maintained on write, and `IF NOT EXISTS` considers it present --
    so the next run will not repair it. Surfacing the name is enough to act on.
    """
    cursor.execute(
        """
        SELECT i.relname
        FROM pg_index x
        JOIN pg_class i ON i.oid = x.indexrelid
        JOIN pg_class t ON t.oid = x.indrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        WHERE NOT x.indisvalid AND n.nspname = %s AND t.relname = %s
        """,
        (schema, collection),
    )
    return [row[0] for row in cursor.fetchall()]


def apply_table_config(collection: str, table_config: dict, vector_secret_name: str) -> dict:
    """Run the table_config statements against the features database."""
    import psycopg2

    from veda_data_pipeline.utils.vector_ingest.handler import get_secret

    statements = build_statements(collection, table_config)
    if not statements:
        print("No table_config provided, skipping table configuration")
        return {"status": "skipped", "statements": []}

    secrets = get_secret(vector_secret_name)
    conn = psycopg2.connect(
        host=secrets["host"],
        dbname=secrets["dbname"],
        user=secrets["username"],
        password=secrets["password"],
    )
    # CREATE INDEX CONCURRENTLY cannot run inside a transaction block.
    conn.autocommit = True

    try:
        with conn.cursor() as cursor:
            for statement in statements:
                print(f"Running: {statement}")
                cursor.execute(statement)

            invalid = find_invalid_indexes(
                cursor, collection, table_config.get("schema", "public")
            )
            if invalid:
                print(
                    f"WARNING: invalid indexes on {collection} (likely an interrupted "
                    f"CONCURRENTLY build); drop and recreate them: {invalid}"
                )
    finally:
        conn.close()

    return {"status": "success", "statements": statements}
