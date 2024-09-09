from __future__ import annotations
from airflow import DAG
from airflow.decorators import task
from airflow.operators.dummy_operator import DummyOperator

DAG_ID = "automate-cog-transformation"

dag_run_config = {
    "data_acquisition_method": "s3",
    "raw_data_bucket": "ghgc-data-store-develop",
    "raw_data_prefix": "delivery/tm54dvar-ch4flux-mask-monthgrid-v5",
    "dest_data_bucket": "ghgc-data-store-develop",
    "cog_data_prefix": "transformed_cogs",
    "collection_name":"tm54dvar-ch4flux-mask-monthgrid-v5",
    "nodata":-9999,
    "ext": ".nc" # .nc, .nc4, .tif, .tiff
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
    def discover_files(ti):
        from dags.automated_transformation.transformation_functions import get_all_s3_keys

        config = ti.dag_run.conf.copy()
        bucket = config.get("raw_data_bucket")
        model_name = config.get("raw_data_prefix")
        ext = config.get("ext") # .nc as well
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
        from dags.automated_transformation.transformation_pipeline import transform_cog

        config = dag_run.conf.copy()
        raw_bucket_name = config.get("raw_data_bucket")
        dest_data_bucket = config.get("dest_data_bucket")
        cog_prefix_name = config.get("cog_data_prefix")
        date_fmt = config.get("date_fmt")
        nodata = config.get("nodata")
        collection_name = config.get("collection_name")
        ext = config.get("ext")
        print(f"The file I am processing is {file_url}")
        print("len of files", len(file_url))
        transform_cog(
            file_url,
            nodata = nodata,
            raw_data_bucket=raw_bucket_name,
            dest_data_bucket=dest_data_bucket,
            cog_data_prefix=cog_prefix_name,
            collection_name=collection_name,
        )
        return None

    # @task
    # def generate_report(report_data, json_filename, **kwargs):
    #     from odiac_processing.processing import upload_json_report

    #     dag_run = kwargs.get("dag_run")
    #     config = dag_run.conf.copy()
    #     bucket_name = config.get("cog_data_bucket")
    #     s3_destination_folder_name = config.get("cog_data_prefix")
    #     report_json_filename = config.get("report_json_filename")
    #     return upload_json_report(
    #         report_data=report_data,
    #         bucket_name=bucket_name,
    #         s3_folder_name=s3_destination_folder_name,
    #         json_filename=report_json_filename,
    #     )

    urls = start >> discover_files()
    report_data = process_files.expand(file_url=urls) >> end
    # generate_report(report_data=report_data[0],
    # json_filename=report_data[1]) >> end
