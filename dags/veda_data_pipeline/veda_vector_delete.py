from veda_data_pipeline.veda_vector_pipeline import invalidate_cloudfront
import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from airflow.models.variable import Variable
from airflow.operators.empty import EmptyOperator
from slack_notifications import slack_fail_alert


dag_run_conf = {
    "collection": Param(
        type="string",
        description="TiPg table name to delete"
    ),
    "schema": Param(
        type="string",
        description="TiPg table schema for collection table",
        default="public",
    ),
    "invalidate_cloudfront": Param(
        type="boolean",
        description="Refresh cache after changes",
        default=True, 
    ),
}


dag_doc_md = """
### Delete Vector Collection DAG
Provides basic `DELETE /collection` behavior not yet supported in API layer. If more 
sophisticated TiPg handling is required, recommend contributing upstream.

This DAG validates that the provided collection is a candidate for programmatic deletion and 
drops the table + dependent objects. Cache invalidation can be configured per-run and is enabled by default.

To modify existing collections, see guidance in vector_ingest on using `append` and `overwrite` fields. Delete logic should only be used for complete erasure.

Users should note:

1. The DAG only deletes TiPg database objects, it does not destroy S3 files or other source data associated with the records.
1. Collection inputs must pass a safety check, whereby tables may be programmtically deleted if 'tagged' with a specific SQL comment during initial ingest. 
    1. Collections not added through the ingest pipeline will be flagged as unavailable for delete.

#### Configuration Parameters

- `collection` (string, required): TiPg table name to delete
- `schema` (string, optional): TiPg table schema for collection table
- `invalidate_cloudfront` (boolean, optional): Refresh cache after changes, default: True
"""


@task
def delete_from_featuresdb(**kwargs):
    from veda_data_pipeline.utils.vector_ingest.handler import COLLECTION_TABLE_COMMENT, get_secret
    import psycopg2

    config = kwargs.get("dag_run").conf.copy()
    collection = config.get("collection")
    schema = config.get("schema")

    airflow_vars_json = Variable.get("aws_dags_variables", deserialize_json=True)
    vector_secret_name = airflow_vars_json.get("VECTOR_SECRET_NAME")

    if not vector_secret_name:
        raise ValueError("Missing required variable VECTOR_SECRET_NAME")

    conn_secrets = get_secret(vector_secret_name)
    conn = psycopg2.connect(
        host=conn_secrets["host"],
        dbname=conn_secrets["dbname"],
        user=conn_secrets["username"],
        password=conn_secrets["password"],
    )

    with conn.cursor() as cur:
        cur.execute(
            f"SELECT obj_description({schema}.{collection}, 'pg_class')",
        )
        comment = cur.fetchone()[0]

    if comment != COLLECTION_TABLE_COMMENT:
        raise ValueError(
            f"Programmatic deletion is only allowed for collection tables, ",
            "which should be identified with a specific comment during initial ingestion. ",
            "`{collection}` does not have the expected safety marker, terminating operation."
        )

    try:
        with conn.cursor() as cur:
            drop_stmt = psycopg2.sql.SQL("DROP TABLE {}.{} CASCADE").format(
                psycopg2.sql.Identifier(schema),
                psycopg2.sql.Identifier(collection),
            )
            cur.execute(drop_stmt)
        conn.commit()
    except:
        conn.rollback()
        raise
    finally:
        conn.close()

    return {"status": "success"}


with DAG(
    "vector_collection_delete", 
    params=dag_run_conf,
    max_active_runs=1,
    doc_md=dag_doc_md,
    schedule=None,
    catchup=False,
    start_date=pendulum.today("UTC").add(days=-1),
    on_failure_callback=slack_fail_alert,
    tags=["collection"],
) as dag:
    start = EmptyOperator(task_id="start", dag=dag)
    end = EmptyOperator(task_id="end", dag=dag)

    start >> delete_from_featuresdb() >> invalidate_cloudfront() >> end
