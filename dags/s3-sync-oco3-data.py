from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import boto3

SRC_BUCKET = "sdap-dev-zarr"
SRC_PREFIX = "OCO3/outputs/v2025.04.16-tfp/v2025.04.16_TFP_cog/"

DST_BUCKET = "ghgc-data-store"
DST_PREFIX = "oco3-co2-sams-daygrid-v11r"

s3 = boto3.client("s3")

def list_filtered_files(bucket, prefix):
    paginator = s3.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if "unfiltere" not in key:
                keys.append(key)
    return keys

def task_list_source(**context):
    files = list_filtered_files(SRC_BUCKET, SRC_PREFIX)
    context["ti"].xcom_push(key="source_files", value=files)

def task_list_target(**context):
    files = list_filtered_files(DST_BUCKET, DST_PREFIX)
    # store only the filenames (basename)
    basenames = [f.split("/")[-1] for f in files]
    context["ti"].xcom_push(key="target_files", value=basenames)

def task_compute_new(**context):
    source_files = context["ti"].xcom_pull(key="source_files")
    target_files = context["ti"].xcom_pull(key="target_files")

    # Compare only filenames
    src_basenames = [f.split("/")[-1] for f in source_files]
    new_files = [
        f for f in source_files
        if f.split("/")[-1] not in target_files
    ]

    context["ti"].xcom_push(key="new_files", value=new_files)

def task_copy_new(**context):
    new_files = context["ti"].xcom_pull(key="new_files")
    count = 0

    for src_key in new_files:
        filename = src_key.split("/")[-1]
        dst_key = f"{DST_PREFIX}/{filename}"

        s3.copy_object(
            Bucket=DST_BUCKET,
            Key=dst_key,
            CopySource={"Bucket": SRC_BUCKET, "Key": src_key}
        )
        count += 1

    print(f"Copied {count} new files.")
    return count


default_args = {
    "owner": "sid",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "s3_sync_new_files",
    default_args=default_args,
    description="Compare S3 buckets and copy only new files",
    schedule_interval="@monthly",
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    list_source = PythonOperator(
        task_id="list_source_files",
        python_callable=task_list_source,
        provide_context=True,
    )

    list_target = PythonOperator(
        task_id="list_target_files",
        python_callable=task_list_target,
        provide_context=True,
    )

    compute_new = PythonOperator(
        task_id="compute_new_files",
        python_callable=task_compute_new,
        provide_context=True,
    )

    copy_new = PythonOperator(
        task_id="copy_new_files",
        python_callable=task_copy_new,
        provide_context=True,
    )

    # DAG structure
    list_source >> list_target >> compute_new >> copy_new
