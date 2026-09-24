from datetime import timedelta
from slack_notifications import slack_fail_alert

import boto3
import pendulum

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models.param import Param
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from airflow.providers.amazon.aws.operators.rds import RdsStartExportTaskOperator
from airflow.providers.amazon.aws.sensors.rds import RdsExportTaskExistenceSensor
from botocore.exceptions import BotoCoreError, ClientError

# Define default arguments
default_args = {"retries": 0}

default_params = {
    "db_id": "your-database-name",
    "export_task_identifier": "your-task-identifier",
    "snapshot_arn": "your-snapshot-arn",
    "glue_role_arn": "your-glue-role-arn",
    "bucket_name": "your-snapshot-bucket-name",
    "s3_prefix": "your-s3-prefix",
    "export_role_arn": "your-export-role-arn",
    "kms_key_id": "your-s3-export-kms-id",
    "paths_excluded": Param(["**/_SUCCESS"], type="array", items={"type": "string"}),
    "export_only": Param([], type="array", items={"type": "string"}),
    "delete_glue_database": Param(True, type="boolean"),
}


def generate_crawl_config(dag_run=None):
    """
    This task is created in case we need
    to perform any business logic on the configuration before submitting the configuration to AWS Crawler.
    """
    config = dag_run.conf
    s3_path = f"{config['bucket_name']}/{config['s3_prefix']}/{config['export_task_identifier']}"
    return {
        "Name": config["export_task_identifier"],
        "Role": config["glue_role_arn"],
        "DatabaseName": config["db_id"],
        "Targets": {
            "S3Targets": [{"Path": s3_path, "Exclusions": config["paths_excluded"]}]
        },
    }


def delete_glue_database_task(dag_run=None):
    client = boto3.client("glue")
    conf = dag_run.conf
    database_id = conf.get("db_id")
    # If the user didn't want to delete Glue database
    # Default to True
    if not conf.get("delete_glue_database", True):
        return

    try:
        response = client.delete_database(Name=database_id)
        print(f"Successfully deleted Glue database: {database_id}")
        return response

    except client.exceptions.EntityNotFoundException:
        # Handle the case where the database does not exist
        print(f"Glue database {database_id} does not exist; no action needed.")
        return {"message": f"Database {database_id} not found; skipping deletion"}

    except (ClientError, BotoCoreError) as e:
        # Handle other boto3-specific exceptions
        print(f"Failed to delete Glue database {database_id}: {e}")
        raise AirflowException(f"Error deleting Glue database {database_id}: {e}")

    except KeyError as e:
        # Handle missing db_id in conf
        print(f"Database ID not found in DAG run configuration: {e}")
        raise AirflowException(f"Database ID missing in DAG configuration: {e}")

    except Exception as e:
        # Catch-all for any other exceptions
        print(f"An unexpected error occurred: {e}")
        raise AirflowException(f"Unexpected error: {e}")


def get_export_only_list_task(dag_run=None):
    conf = dag_run.conf
    export_only = conf["export_only"]
    export_only = export_only if export_only != ["null"] else []
    return export_only


# Airflow DAG definition with doc_md documentation
with DAG(
    dag_id="rds_s3_export_snapshots",
    default_args=default_args,
    tags=["RDS", "Operations", "Disaster Recovery", "Long Term"],
    schedule=None,
    start_date=pendulum.today("UTC").add(days=-1),
    max_active_runs=4,  # Only 5 parallel exports are allowed
    catchup=False,
    params=default_params,
    on_failure_callback=slack_fail_alert,
    render_template_as_native_obj=True,
    doc_md=f"""
        ### RDS to S3 Snapshot Export and S3 Data Crawling
        This DAG exports an RDS snapshot to S3 and uses AWS Glue to crawl the exported data,
        making it accessible for querying. The process involves:

        ## Workflow
        1. **Export RDS Snapshot**: Initiates an export of the snapshot from RDS to S3 using `RdsStartExportTaskOperator`.
        2. **Monitor Export Completion**: Uses `RdsExportTaskExistenceSensor` to monitor export completion.
        3. **Delete Glue Database**: Deletes any existing Glue database before recreating it.
        4. **Run Glue Crawler**: Initiates a Glue Crawler on the exported S3 data for indexing.

        **Parameters**:
        ```json

        {default_params}

        ```
        """,
) as dag:
    # Start and end markers for DAG tasks
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    get_export_only = PythonOperator(
        task_id="get_export_only", python_callable=get_export_only_list_task
    )

    # Task to start export of RDS snapshot to S3
    start_s3_export = RdsStartExportTaskOperator(
        task_id="start_export",
        export_task_identifier="{{ dag_run.conf['export_task_identifier'] }}",
        source_arn="{{ dag_run.conf['snapshot_arn'] }}",
        s3_bucket_name="{{ dag_run.conf['bucket_name'] }}",
        s3_prefix="{{ dag_run.conf['s3_prefix'] }}",
        iam_role_arn="{{ dag_run.conf['export_role_arn'] }}",
        kms_key_id="{{ dag_run.conf['kms_key_id'] }}",
        export_only="{{ ti.xcom_pull('get_export_only') }}",
        wait_for_completion=False,
    )

    # Sensor to check for completion of RDS export
    export_sensor = RdsExportTaskExistenceSensor(
        task_id="export_sensor",
        export_task_identifier="{{ dag_run.conf['export_task_identifier'] }}",
        target_statuses=["complete"],
        timeout=timedelta(minutes=60),
    )
    # Task to delete the database
    delete_glue_database = PythonOperator(
        task_id="delete_glue_database", python_callable=delete_glue_database_task
    )
    generate_crawl_config_task = PythonOperator(
        task_id="generate_crawl_config", python_callable=generate_crawl_config
    )

    # Task to initiate AWS Glue Crawler on the exported S3 data
    run_crawl_s3 = GlueCrawlerOperator(
        task_id="crawl_s3",
        config="{{ ti.xcom_pull('generate_crawl_config') }}",
    )

    # Setting up the task sequence
    (
        start
        >> get_export_only
        >> start_s3_export
        >> export_sensor
        >> delete_glue_database
        >> generate_crawl_config_task
        >> run_crawl_s3
        >> end
    )
