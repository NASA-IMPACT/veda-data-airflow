from __future__ import annotations

from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from slack_notifications import slack_fail_alert
from airflow.models.variable import Variable
from veda_data_pipeline.utils.xcom_to_s3 import write_xcom_to_s3,read_xcom_from_s3
import re

DAG_ID = "automate-cog-transformation"

# Custom validation function


dag_run_config = {
    "data_acquisition_method": Param(
        "s3", enum=["s3"]
    ),  # To add Other protocols (HTTP, SFTP...)
    "plugins_uri": Param(
        "https://raw.githubusercontent.com/US-GHG-Center/ghgc-docs/refs/heads/main/",
        type="string",
    ),
    "raw_data_bucket": "ghgc-data-store-develop",
    "raw_data_prefix": Param(
        "delivery/gpw",
        type="string",
        pattern="^[^/].*[^/]$",
    ),
    # Add a regex pattern after the prefix to filter the raw files
    "raw_data_filter_regex": Param(".*.nc$", type="string"),
    "dest_data_bucket": "ghgc-data-store-develop",
    "data_prefix": Param("transformed_cogs", type="string", pattern="^[^/].*[^/]$"),
    "collection_name": Param("gpw", type="string"),
    "nodata": Param(-9999, type="number"),
    "ext": Param(".nc", type="string", pattern="^\\..*$"),
    "max_parallel_processing": Param(10, type="integer"),
    "chunk_limit": Param(100, type="integer")
}
dag_doc_md = """

### Automate COG Transformation

This DAG automates the transformation of raw geospatial data into Cloud-Optimized GeoTIFFs (COGs). It fetches transformation plugins, discovers files, processes them, and generates a report.

#### DAG Configuration

```json
{
    "data_acquisition_method": "s3",
    "plugins_uri": "https://raw.githubusercontent.com/US-GHG-Center/ghgc-docs/refs/heads/main/",
    "raw_data_bucket": "ghgc-data-store-develop",
    "raw_data_prefix": "delivery/gpw",
    "dest_data_bucket": "ghgc-data-store-develop",
    "data_prefix": "transformed_cogs",
    "collection_name": "gpw",
    "nodata": -9999,
    "ext": ".nc",
    "max_parallel_processing": 10,
    "chunk_limit": 100
}
"""

with DAG(
        dag_id=DAG_ID,
        schedule=None,
        catchup=False,
        tags=["Transformation", "Report"],
        params=dag_run_config,
        doc_md=dag_doc_md,
        on_failure_callback=slack_fail_alert,
        max_active_runs = 1 # Ensure only one DAG at a time to avoid memory issues (code -9)
) as dag:
    start = EmptyOperator(task_id="start", dag=dag)
    end = EmptyOperator(task_id="end", dag=dag)


    @task
    def check_function_exists(ti):
        from dags.automated_transformation.transformation_pipeline import (
            check_file_exists,
        )

        config = ti.dag_run.conf
        folder_name = "data_transformation_plugins"
        file_name = f'{config.get("collection_name")}_transformation.py'
        try:
            plugin_url = f"{config['plugins_uri'].strip('/')}/{folder_name}/{file_name}"
            check_file_exists(url=plugin_url)
            return f"The {file_name} exists in {folder_name} in this URL {plugin_url}."
        except Exception as e:
            raise Exception(f"Error checking file existence: {e}")

    @task()
    def set_max_active_processing(**kwargs):
        from time import sleep
        dag_run = kwargs.get("dag_run")
        config = dag_run.conf.copy()
        max_parallel_value_stored = Variable.get("max_parallel_processing", default_var=10)
        max_parallel_value_configured = config.get("max_parallel_processing", 10)
        if max_parallel_value_stored != max_parallel_value_configured:
            Variable.set("max_parallel_processing", max_parallel_value_configured)
            # Give time for the scheduler to catch up
            sleep(15)
        return max_parallel_value_configured

    @task
    def discover_files(ti):
        from dags.automated_transformation.transformation_pipeline import (
            get_all_s3_keys,
        )

        config = ti.dag_run.conf.copy()
        bucket = config.get("raw_data_bucket")
        raw_data_prefix = config.get("raw_data_prefix")
        raw_data_regex = config.get("raw_data_filter_regex")
        ext = config.get("ext")  # .nc as well
        generated_list = get_all_s3_keys(bucket, raw_data_prefix, ext)
        collection_name = config.get("collection_name")
        print(f"[ TOTAL DISCOVERED : {len(generated_list)}]")

        # Filter by raw data regex
        pattern = rf"{raw_data_prefix}/{raw_data_regex}"
        filtered_files = [
            f for f in generated_list if re.match(pattern, f)
        ]
        print(f"[ FILTERED BY PATTERN {pattern} : {len(filtered_files)}]")

        # Write this to s3
        bucket_output = Variable.get("EVENT_BUCKET")
        key = f"s3://{bucket_output}/events/{collection_name}"
        chunks_xcom = []
        chunk_limit = min(int(config.get("chunk_limit", 100)), 900)
        chunk_size = int(len(filtered_files) / chunk_limit) + 1
        for indx, i in enumerate(range(0, len(filtered_files), chunk_size)):
            tmp = filtered_files[i: i + chunk_size]
            output_key = write_xcom_to_s3(f"{key}/chunk_{indx}", tmp)
            chunks_xcom.append(output_key)
        return chunks_xcom


    @task(max_active_tis_per_dag=int(Variable.get("max_parallel_processing", default_var=10)))
    def process_files(s3_url, **kwargs):
        dag_run = kwargs.get("dag_run")
        from dags.automated_transformation.transformation_pipeline import transform_cog

        config = dag_run.conf.copy()
        raw_bucket_name = config.get("raw_data_bucket")
        dest_data_bucket = config.get("dest_data_bucket")
        data_prefix = config.get("data_prefix")
        nodata = config.get("nodata")
        collection_name = config.get("collection_name")
        folder_name = "data_transformation_plugins"
        file_name = f"{collection_name}_transformation.py"
        plugin_url = f"{config['plugins_uri'].strip('/')}/{folder_name}/{file_name}"

        # Get the files url from the s3 location
        file_url_list = read_xcom_from_s3(s3_url)

        print("Total files to process in this task :  ",len(file_url_list))
        file_status = transform_cog(
            file_url_list,
            plugin_url=plugin_url,
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
        count, failed_files = 0, []
        flattened_reports = [report for sublist in reports for report in sublist]
        for report in flattened_reports:
            if "failed" in report.get("status"):
                failed_files.append(report)
            elif "success" in report.get("status"):
                count += 1

        if failed_files:
            raise Exception(f"Error generating {len(failed_files)} COG files. Top 5 failed files : {failed_files[:5]}")
        summary = {
            "collection": collection_name,
            "successes": count,
            "failures": len(failed_files)
        }
        print(summary)

    # @task
    # def report_failure(statuses: list):
    #     all_failures = list()
    #     for status in statuses:
    #         all_failures += status.get('failures', [])
    #     if all_failures:
    #         print(f"Top 10 failed file: {all_failures[:10]}")
    #         raise  Exception(f"Detected {len(all_failures)} errors")


    s3_urls = start >> check_function_exists() >> set_max_active_processing()>> discover_files()
    report_data = process_files.expand(s3_url=s3_urls)
    statuses = generate_report(reports=report_data)
    #report_failure(statuses=statuses) >> end
