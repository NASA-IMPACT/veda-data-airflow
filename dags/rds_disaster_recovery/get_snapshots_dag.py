import hashlib
import json
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

import boto3
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow_multi_dagrun.operators import TriggerMultiDagRunOperator
from botocore.exceptions import BotoCoreError, ClientError


def generate_hash(input_string: str) -> str:
    """
    Generates a unique hash string prefixed with 'ab' from an input string.

    Parameters:
        input_string (str): The input string to hash.

    Returns:
        str: The generated hash, prefixed to start with a letter.
    """
    now = datetime.now()
    md5_hash = hashlib.md5()
    md5_hash.update(input_string.encode("utf-8"))
    # Must start with a letter
    # We need timestamp to sort snapshots
    return f"a{md5_hash.hexdigest()[:17]}-{now.strftime('%Y-%m-%d-%Hh%Mm%Ss')}"


def notify_missing_snapshots_task(ti):
    get_rds_snapshots_xcom = ti.xcom_pull("get_rds_snapshots")
    missing_snapshots = get_rds_snapshots_xcom.get("missing_snapshots")
    if missing_snapshots:
        raise AirflowException(f"Missing Snapshots for RDS: {missing_snapshots}")
    return True


doc_get_snapshots_dag_md_DAG = """
### RDS Snapshot Retrieval and Export
#### Overview
This DAG retrieves the most recent automated snapshots for specified Amazon RDS databases, including both instances and 
clusters. For each snapshot found, the DAG prepares necessary metadata and configuration for exporting the snapshot to 
an S3 bucket. This process supports disaster recovery and backup requirements.

#### Workflow
1. **Fetch Database Snapshots**: Retrieves snapshots created within the past 24 hours.
2. **Check for Missing Snapshots**: Raises an alert if any snapshots are missing.
3. **Trigger Export DAG**: Initiates the downstream DAG to export snapshots to S3.

#### Configuration Parameters
- **Cluster Databases**: Databases specified in `cluster_databases` (Aurora clusters) will have their recent snapshots
 processed.
- **Instance Databases**: Databases specified in `instance_databases` (RDS instances) will have their recent snapshots
 processed.

#### Example Configuration
```json
{"cluster_databases": ['smallsat-uah-staging-aurora-rds'], "instance_databases": ['dms-test-vishal'],
 "paths_excluded": ["**/_SUCCESS"], "export_only": ["database.schema.table"]}
```

This DAG is intended to be used as part of a disaster recovery strategy to ensure regular backups of key RDS 
databases are available in S3."""

dag_params = {
    "snapshots_age_in_hours": Param(24, type="integer", title="Snapshot age in hours"),
    "cluster_databases": Param(
        ["null"], type="array", title="Cluster Databases", items={"type": "string"}
    ),
    "instance_databases": Param(
        ["null"], type="array", title="Instance Databases", items={"type": "string"}
    ),
    "paths_excluded": Param(["**/_SUCCESS"], type="array", items={"type": "string"}),
    "export_only": Param({}, type=["object"]),
    "catalog_db_name": Param("null", type="string"),
    "delete_catalog_db": Param(True, type="boolean"),
    "eager_delete_glue_catalog": Param(False, type="boolean"),
}


def get_snapshots(
    rds_client,
    db_id: str,
    current_time: datetime,
    instance_type: str,
    snapshots_age_in_hours: int = 24,
    paths_excluded: Optional[List[str]] = None,
    export_only: Optional[Dict | None] = None,
) -> dict:
    """
    Fetches the most recent snapshots for a specified database instance or cluster.

    Parameters:
        rds_client: Boto3 RDS client for interacting with AWS RDS.
        db_id (str): The database identifier.
        current_time (datetime): The current time in UTC.
        instance_type (str): Type of the instance ('instance' or 'cluster').
        snapshots_age_in_hours (int): Snapshot created on the last 24 hours
        paths_excluded: Exclude the following list of paths from the crawler
        export_only: Decide what you want to export from RDS. If empty list mean export everything

    Returns:
        dict: discovered snapshots and missing snapshots
    """

    # Handle mutable list issue:
    if not paths_excluded:
        paths_excluded = [""]
    funct = rds_client.describe_db_snapshots
    identifier = "DBInstanceIdentifier"
    snapshot_type = "DBSnapshots"
    snapshot_identifier = "DBSnapshotIdentifier"
    snapshot_arn = "DBSnapshotArn"

    if instance_type == "cluster":
        funct = rds_client.describe_db_cluster_snapshots
        identifier = "DBClusterIdentifier"
        snapshot_identifier = "DBClusterSnapshotIdentifier"
        snapshot_type = "DBClusterSnapshots"
        snapshot_arn = "DBClusterSnapshotArn"

    missing_snapshots = []
    snapshots = []
    try:
        response = funct(**{identifier: db_id, "SnapshotType": "automated"})
        if response[snapshot_type]:
            snapshots += [
                {
                    "instance_type": instance_type,
                    "db_id": db_id,
                    "db_snapshot_id": snapshot[snapshot_identifier],
                    "snapshot_arn": snapshot[snapshot_arn],
                    "paths_excluded": paths_excluded,
                    "export_only": export_only,
                }
                for snapshot in response[snapshot_type]
                if current_time - snapshot["SnapshotCreateTime"]
                < timedelta(hours=snapshots_age_in_hours)
            ]
        else:
            missing_snapshots.append(
                {"database_name": db_id, "reason": "Missing Snapshot"}
            )
    except Exception as ex:
        missing_snapshots.append({"database_name": db_id, "reason": str(ex)[:50]})

    return {"existing_snapshots": snapshots, "missing_snapshots": missing_snapshots}


def get_snapshots_task(ti):
    """
    Retrieves RDS snapshots information for clusters and instances as configured.

    Returns:
        dict: Snapshot configuration with relevant metadata and AWS resource identifiers.
    """
    config = ti.dag_run.conf

    try:
        rds_client = boto3.client("rds")
    except ClientError as e:
        ti.log.error("Error creating RDS client: %s", e)
        return {"existing_snapshots": [], "missing_snapshots": []}

    # Retrieve database names depending on how the DAG was triggered
    if ti.dag_run.external_trigger:
        snapshots_age_in_hours = int(config.get("snapshots_age_in_hours", 24))
        cluster_databases = config.get("cluster_databases", [])
        instance_databases = config.get("instance_databases", [])
        paths_excluded = config.get("paths_excluded")
        export_only = config.get("export_only")
    else:
        var_json = Variable.get("aws_dags_variables", deserialize_json=True)
        cluster_databases = var_json.get("cluster_databases", "null").split(",")
        instance_databases = var_json.get("instance_databases", "null").split(",")
        snapshots_age_in_hours = int(var_json.get("snapshots_age_in_hours", 24))
        paths_excluded = var_json.get("paths_excluded", "**/_SUCCESS").split(",")
        export_only = json.loads(var_json.get("export_only", {}))

    # Clean up the database lists in case of "null" or empty values
    cluster_databases = [db for db in cluster_databases if db and db != "null"]
    instance_databases = [db for db in instance_databases if db and db != "null"]

    current_time = datetime.now(timezone.utc)
    snapshots = []
    missing_snapshots = []
    common_kwargs = {
        "rds_client": rds_client,
        "current_time": current_time,
        "snapshots_age_in_hours": snapshots_age_in_hours,
        "paths_excluded": paths_excluded,
        "export_only": export_only,
    }

    for cluster_db in cluster_databases:
        cluster_snapshots_discovered = get_snapshots(
            **common_kwargs,
            db_id=cluster_db,
            instance_type="cluster",
        )
        snapshots.extend(cluster_snapshots_discovered.get("existing_snapshots", []))
        missing_snapshots.extend(
            cluster_snapshots_discovered.get("missing_snapshots", [])
        )

    for instance_db in instance_databases:
        instance_snapshots_discovered = get_snapshots(
            **common_kwargs, db_id=instance_db, instance_type="instance"
        )
        snapshots.extend(instance_snapshots_discovered.get("existing_snapshots", []))
        missing_snapshots.extend(
            instance_snapshots_discovered.get("missing_snapshots", [])
        )

    return {"existing_snapshots": snapshots, "missing_snapshots": missing_snapshots}


def trigger_s3_export_dag_task(**kwargs) -> dict:
    """
    Retrieves RDS snapshots information for clusters and instances as configured.

    Returns:
        dict: Snapshot configuration with relevant metadata and AWS resource identifiers.
    """
    ti = kwargs["ti"]
    conf = ti.dag_run.conf
    var_json = Variable.get("aws_dags_variables", deserialize_json=True)
    get_rds_snapshots_xcom = ti.xcom_pull("get_rds_snapshots")
    snapshots = get_rds_snapshots_xcom.get("existing_snapshots", [])
    for snapshot in snapshots:
        yield {
            "run_id": f"{ti.dag_run.run_id}-{snapshot['db_id']}",
            "db_id": (
                conf.get("catalog_db_name")
                if conf.get("catalog_db_name") != "null"
                else snapshot["db_id"]
            ),
            "export_task_identifier": generate_hash(snapshot["db_snapshot_id"]),
            "snapshot_arn": snapshot["snapshot_arn"],
            "export_role_arn": var_json["S3_EXPORT_ROLE_ARN"],
            "glue_role_arn": var_json["GLUE_ROLE_ARN"],
            "bucket_name": var_json["SNAPSHOT_BUCKET_NAME"],
            "s3_prefix": f"rds-snapshots/{snapshot['db_id']}",
            "kms_key_id": var_json["S3_EXPORT_KMS_KEY_ID"],
            "paths_excluded": snapshot["paths_excluded"],
            "export_only": snapshot["export_only"].get(snapshot["db_id"], []),
            "delete_glue_database": conf.get("delete_catalog_db"),
        }


# Define default arguments
default_args = {"retries": 0, "start_date": days_ago(1), "catchup": False}


def delete_glue_database_task(ti):
    client = boto3.client("glue")
    conf = ti.dag_run.conf
    database_id = conf.get("catalog_db_name")
    # If the user didn't want to delete Glue database
    # Default to True
    if not conf.get("eager_delete_glue_catalog", True):
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


with DAG(
    dag_id="get_rds_snapshots",
    schedule=None,  # We can run it manually and as needed #"0 0 L * *",  # Run on the last day of each month
    doc_md=doc_get_snapshots_dag_md_DAG,
    params=dag_params,
    default_args=default_args,
    tags=["RDS", "CSDAP", "Disaster Recovery", "Trigger Export", "Long Term"],
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    get_rds_snapshots = PythonOperator(
        task_id="get_rds_snapshots", python_callable=get_snapshots_task
    )

    rds_snapshots_dag_run = TriggerMultiDagRunOperator(
        task_id="trigger_multi_s3_export_dag",
        dag=dag,
        trigger_dag_id="rds_s3_export_snapshots",
        python_callable=trigger_s3_export_dag_task,
    )

    # Task to eagerly delete Glue database
    # Needed for csda-ops-tool client
    eager_delete_glue_database = PythonOperator(
        task_id="eager_delete_glue_database", python_callable=delete_glue_database_task
    )

    notify_missing_snapshots = PythonOperator(
        task_id="notify_missing_snapshots",
        python_callable=notify_missing_snapshots_task,
    )

    (
        start
        >> get_rds_snapshots
        >> eager_delete_glue_database
        >> rds_snapshots_dag_run
        >> notify_missing_snapshots
        >> end
    )