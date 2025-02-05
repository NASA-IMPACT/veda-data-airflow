from __future__ import annotations

from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from airflow.operators.dummy_operator import DummyOperator
from slack_notifications import slack_fail_alert

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
    "raw_data_filter_regex": ".*.nc$"
    "dest_data_bucket": "ghgc-data-store-develop",
    "data_prefix": Param("transformed_cogs", type="string", pattern="^[^/].*[^/]$"),
    "collection_name": Param("gpw", type="string"),
    "nodata": Param(-9999, type="number"),
    "ext": Param(".nc", type="string", pattern="^\\..*$"),
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
    "ext": ".nc"
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
) as dag:
    start = DummyOperator(task_id="start", dag=dag)
    end = DummyOperator(task_id="end", dag=dag)


    @task
    def check_function_exists(ti):
        from dags.automated_transformation.transformation_pipeline import (
            download_python_file,
        )

        config = ti.dag_run.conf
        folder_name = "data_transformation_plugins"
        file_name = f'{config.get("collection_name")}_transformation.py'
        try:
            plugin_url = f"{config['plugins_uri'].strip('/')}/{folder_name}/{file_name}"
            download_python_file(uri=plugin_url)
            return f"The {file_name} exists in {folder_name} in this URL {plugin_url}."
        except Exception as e:
            raise Exception(f"Error checking file existence: {e}")


    @task
    def discover_files(ti):
        from dags.automated_transformation.transformation_pipeline import (
            get_all_s3_keys,
        )

        config = ti.dag_run.conf.copy()
        bucket = config.get("raw_data_bucket")
        data_prefix = config.get("raw_data_prefix")
        ext = config.get("ext")  # .nc as well
        generated_list = get_all_s3_keys(bucket, data_prefix, ext)
        chunk_size = int(len(generated_list) / 900) + 1
        return [
            generated_list[i: i + chunk_size]
            for i in range(0, len(generated_list), chunk_size)
        ]

    @task
    def filter_discovered_files(files_chunk, ti):
        config = ti.dag_run.conf
        raw_data_regex = config.get("raw_data_filter_regex")
        raw_data_prefix = config.get("raw_data_prefix")
        pattern = rf"{raw_data_prefix}{raw_data_regex}"
        filtered_files = [
            f for f in files_chunk if re.match(pattern, f)
        ]
        return filtered_files


    @task(max_active_tis_per_dag=1)
    def process_files(file_url, **kwargs):
        dag_run = kwargs.get("dag_run")
        from dags.automated_transformation.transformation_pipeline import transform_cog

        config = dag_run.conf.copy()
        raw_bucket_name = config.get("raw_data_bucket")
        dest_data_bucket = config.get("dest_data_bucket")
        data_prefix = config.get("data_prefix")
        nodata = config.get("nodata")
        collection_name = config.get("collection_name")
        print(f"The file I am processing is {file_url}")
        print("len of files", len(file_url))
        folder_name = "data_transformation_plugins"
        file_name = f"{collection_name}_transformation.py"
        plugin_url = f"{config['plugins_uri'].strip('/')}/{folder_name}/{file_name}"

        file_status = transform_cog(
            file_url,
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
        for report in reports:
            if "failed" in report.values():
                failed_files.append(report)
            elif "success" in report.values():
                count += 1

        if failed_files:
            raise Exception(f"Error generating COG file {failed_files}")
        return {
            "collection": collection_name,
            "successes": count,
            "failures": failed_files,
        }


    filtered_urls = start >> check_function_exists() >> discover_files() >> filter_discovered_files()
    report_data = process_files.expand(file_url=filtered_urls)
    generate_report(reports=report_data) >> end
