from __future__ import annotations

import importlib

from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from airflow.operators.dummy_operator import DummyOperator

DAG_ID = "automate-cog-transformation"

# Custom validation function


dag_run_config = {
    "data_acquisition_method": Param(
        "s3", enum=["s3"]
    ),  # To add Other protocols (HTTP, SFTP...)
    "raw_data_bucket": "ghgc-data-store-develop",
    "raw_data_prefix": Param(
        "delivery/tm54dvar-ch4flux-mask-monthgrid-v5",
        type="string",
        pattern="^[^/].*[^/]$",
    ),
    "dest_data_bucket": "ghgc-data-store-develop",
    "data_prefix": Param("transformed_cogs", type="string", pattern="^[^/].*[^/]$"),
    "collection_name": "tm54dvar-ch4flux-mask-monthgrid-v5",
    "nodata": Param(-9999, type="numbers"),
    "ext": Param(".nc", type="string", pattern="^\\..*$"),
}

with DAG(
    dag_id=DAG_ID,
    schedule=None,
    catchup=False,
    tags=["Transformation", "Report"],
    params=dag_run_config,
) as dag:
    start = DummyOperator(task_id="start", dag=dag)
    end = DummyOperator(task_id="end", dag=dag)

    @task
    def check_function_exists(ti):
        config = ti.dag_run.conf.copy()
        collection_name = config.get("collection_name")
        module = importlib.import_module(
            "automated_transformation.transformation_functions"
        )
        function_name = f'{collection_name.replace("-", "_")}_transformation'
        if not hasattr(module, function_name):
            raise Exception(
                f"The function {function_name} does not exist in the module {module}."
            )
        return f"The function {function_name} does not exist in the module {module}."

    @task
    def discover_files(ti):
        from dags.automated_transformation.transformation_functions import \
            get_all_s3_keys

        config = ti.dag_run.conf.copy()
        bucket = config.get("raw_data_bucket")
        model_name = config.get("raw_data_prefix")
        ext = config.get("ext")  # .nc as well
        # return get_all_s3_keys(bucket, model_name, ext)
        generated_list = get_all_s3_keys(bucket, model_name, ext)
        chunk_size = int(len(generated_list) / 900) + 1
        return [
            generated_list[i : i + chunk_size]
            for i in range(0, len(generated_list), chunk_size)
        ]

    @task(max_active_tis_per_dag=10)
    def process_files(file_url, **kwargs):
        dag_run = kwargs.get("dag_run")
        from dags.automated_transformation.transformation_pipeline import \
            transform_cog

        config = dag_run.conf.copy()
        raw_bucket_name = config.get("raw_data_bucket")
        dest_data_bucket = config.get("dest_data_bucket")
        data_prefix = config.get("data_prefix")
        nodata = config.get("nodata")
        collection_name = config.get("collection_name")
        print(f"The file I am processing is {file_url}")
        print("len of files", len(file_url))
        file_status = transform_cog(
            file_url,
            nodata=nodata,
            raw_data_bucket=raw_bucket_name,
            dest_data_bucket=dest_data_bucket,
            data_prefix=data_prefix,
            collection_name=collection_name,
        )
        return file_status

    @task
    def generate_report(reports, **kwargs):
        dag_run = kwargs.get("dag_run")
        collection_name = dag_run.conf.get("collection_name")
        return {"collection": collection_name, "successes": len(reports)}

    urls = start >> check_function_exists() >> discover_files()
    report_data = process_files.expand(file_url=urls)
    generate_report(reports=report_data) >> end
