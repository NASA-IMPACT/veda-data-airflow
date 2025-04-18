from __future__ import annotations
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, date
import json
from airflow.decorators import task


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError("Type %s not serializable" % type(obj))


# [START postgres_operator_howto_guide]


# create_pet_table, populate_pet_table, get_all_pets, and get_birth_date are examples of tasks created by
# instantiating the Postgres Operator

DAG_ID = "postgres_operator_dag"


with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2020, 2, 2),
    schedule="@once",
    catchup=False,
    tags=["example"],
) as dag:
    # [START postgres_operator_howto_guide_create_pet_table]
    create_pet_table = PostgresOperator(
        postgres_conn_id="cluster_rds_connection",
        task_id="create_pet_table",
        sql="""
            CREATE TABLE IF NOT EXISTS pet (
            pet_id SERIAL PRIMARY KEY,
            name VARCHAR NOT NULL,
            pet_type VARCHAR NOT NULL,
            birth_date DATE NOT NULL,
            OWNER VARCHAR NOT NULL);
          """,
    )
    # [END postgres_operator_howto_guide_create_pet_table]
    # [START postgres_operator_howto_guide_populate_pet_table]
    populate_pet_table = PostgresOperator(
        postgres_conn_id="cluster_rds_connection",
        task_id="populate_pet_table",
        sql="""
            INSERT INTO pet (name, pet_type, birth_date, OWNER)
            VALUES ( 'Max', 'Dog', '2018-07-05', 'Jane');
            INSERT INTO pet (name, pet_type, birth_date, OWNER)
            VALUES ( 'Susie', 'Cat', '2019-05-01', 'Phil');
            INSERT INTO pet (name, pet_type, birth_date, OWNER)
            VALUES ( 'Lester', 'Hamster', '2020-06-23', 'Lily');
            INSERT INTO pet (name, pet_type, birth_date, OWNER)
            VALUES ( 'Quincy', 'Parrot', '2013-08-11', 'Anne');
            """,
    )
    # [END postgres_operator_howto_guide_populate_pet_table]
    # [START postgres_operator_howto_guide_get_all_pets]

    @task
    def get_all_pets():
        sql = "SELECT * FROM pet"
        pg_hook = PostgresHook(postgres_conn_id="cluster_rds_connection")
        connection = pg_hook.get_conn()
        cursor = connection.cursor()
        cursor.execute(sql)
        results = cursor.fetchall()
        for result in results:
            print(result)
        return {"results": json.dumps(results, default=json_serial)}

    # [END postgres_operator_howto_guide_get_all_pets]
    # [START postgres_operator_howto_guide_get_birth_date]
    get_birth_date = PostgresOperator(
        postgres_conn_id="cluster_rds_connection",
        task_id="get_birth_date",
        sql="SELECT * FROM pet WHERE birth_date BETWEEN SYMMETRIC %(begin_date)s AND %(end_date)s",
        parameters={"begin_date": "2020-01-01", "end_date": "2020-12-31"},
        runtime_parameters={"statement_timeout": "3000ms"},
    )
    # [END postgres_operator_howto_guide_get_birth_date]

    create_pet_table >> populate_pet_table >> get_all_pets() >> get_birth_date
    # [END postgres_operator_howto_guide]
