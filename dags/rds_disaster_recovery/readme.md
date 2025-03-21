
# DAG Documentation for `get_rds_snapshots`

## Overview
This setup includes two DAGs [get_snapshots_dag](../../dags/rds_disaster_recovery/get_snapshots_dag.py) and 
[rds_s3_export](../../dags/rds_disaster_recovery/rds_s3_export.py) for handling the retrieval and export of RDS
snapshots to S3, as well as metadata indexing through AWS Glue. This is intended as part of a disaster recovery strategy
to ensure regular backups of key RDS databases are available in S3 and are queryable through AWS Glue.
## Architecture 
![get_rds_snapshots_dag_architecture](imgs/get_snapshots_dag_architecture.png)

## Workflow Steps for get_snapshots_dag DAG

1. **Start**: Marks the beginning of the DAG execution.
2. **Fetch Database Snapshots**: Using the `get_snapshots_task` function, retrieves snapshots created within the last 24 hours for RDS databases specified in the DAG parameters.
3. **Notify on Missing Snapshots**: If no recent snapshots are found for specified databases, the `notify_missing_snapshots_task` will raise an exception, signaling an alert for missing snapshots.
4. **Trigger Export DAG**: The `trigger_s3_export_dag_task` function initiates the `rds_s3_export_snapshots` DAG, passing the necessary metadata for exporting the snapshots to S3.
5. **End**: Marks the completion of the DAG execution.

## Configuration Parameters
The DAG can be customized with the following parameters:

- **snapshots_age_in_hours**: Defines the age threshold (in hours) for recent snapshots to be retrieved (default is 24 hours).
- **cluster_databases**: List of Aurora cluster identifiers for which snapshots should be retrieved.
- **instance_databases**: List of RDS instance identifiers for which snapshots should be retrieved.
- **paths_excluded**: List of S3 paths to be excluded by AWS Glue Crawler.
- **export_only**: A dictionary to specify particular database tables to export

### Example Configuration:
```json
{
  "snapshots_age_in_hours": 24,
  "cluster_databases": ["cluster_db_id"],
  "instance_databases": ["instance_db_id"],
  "paths_excluded": ["List of paths to exclude"],
  "export_only": {"instance/clusetr_name": ["database.schema.table1", "database.schema.table2"]  }
}
```

## Functions

### `generate_hash(input_string: str) -> str`
Generates a unique hash prefixed with a letter from an input string and appends a timestamp. Used for export task identifiers.

### `notify_missing_snapshots_task(ti)`
Raises an exception if no recent snapshots are found for specified databases, notifying that required snapshots are missing.

### `get_snapshots_task(ti)`
Retrieves recent snapshots for specified databases and clusters. This function communicates with AWS RDS and returns any snapshots created within the specified `snapshots_age_in_hours`.

### `trigger_s3_export_dag_task(**kwargs) -> dict`
Triggers the downstream DAG (`rds_s3_export_snapshots`) for each snapshot to be exported. Uses metadata from retrieved snapshots and configuration variables from Airflow to create export tasks.

## Task Dependencies
The tasks within this DAG are executed in the following order:

1. `start`
2. `get_rds_snapshots`
3. `trigger_multi_s3_export_dag`
4. `notify_missing_snapshots`
5. `end`

## Default Arguments
- **retries**: 0
- **start_date**: Set to the current day with `days_ago(0)` (to be updated if scheduling changes).
- **catchup**: False

## Tags
The DAG is tagged with `RDS`, `CSDAP`, `Disaster Recovery`, `Trigger Export`, and `Long Term`, aiding in categorizing the workflow for monitoring and maintenance.

## Additional Notes
This DAG forms part of a disaster recovery strategy by regularly exporting RDS snapshots to an S3 bucket, thus enabling long-term storage and retrieval for key RDS databases. The export task configuration leverages AWS IAM roles, S3 bucket permissions, and KMS keys, configured within Airflow Variables.


## Workflow Steps for rds_s3_export DAG
## Overview
This DAG exports an RDS snapshot to S3, then uses an AWS Glue crawler to index the exported data, making it available for querying. This complements the first DAG, which triggers this export operation.

## Purpose
This DAG is designed to export RDS snapshots to S3 and initiate Glue crawling to make the data queryable, thereby enhancing disaster recovery capabilities.

## Workflow
1. **Export RDS Snapshot**: Initiates an export of the snapshot from RDS to S3 using `RdsStartExportTaskOperator`.
2. **Monitor Export Completion**: Uses `RdsExportTaskExistenceSensor` to monitor export completion.
3. **Delete Glue Database**: Deletes any existing Glue database before recreating it.
4. **Run Glue Crawler**: Initiates a Glue Crawler on the exported S3 data for indexing.

## Configuration Parameters
- **db_id**: Glue database identifier.
- **export_task_identifier**: Unique identifier for the export task.
- **snapshot_arn**: ARN of the RDS snapshot to export.
- **glue_role_arn**: IAM role for the Glue Crawler.
- **bucket_name**: S3 bucket name to store the snapshot.
- **s3_prefix**: Prefix in S3 bucket for snapshot storage.
- **export_role_arn**: IAM role to export the snapshot to S3.
- **kms_key_id**: KMS key ID to encrypt the snapshot in S3.
- **paths_excluded**: List of S3 paths to be excluded by AWS Glue Crawler.


## Example Configuration
```json
{
    "db_id": "your-database-name",
    "export_task_identifier": "your-task-identifier",
    "snapshot_arn": "your-snapshot-arn",
    "glue_role_arn": "your-glue-role-arn",
    "bucket_name": "your-snapshot-bucket-name",
    "s3_prefix": "your-s3-prefix",
    "export_role_arn": "your-export-role-arn",
    "kms_key_id": "your-s3-export-kms-id",
    "paths_excluded": ["List of paths to exclude"],
    "export_only": ["databsename.schema.table"]
}
```

## Execution Flow
1. **Start**: Marks the beginning of the DAG.
2. **Start S3 Export**: Initiates the snapshot export process.
3. **Export Sensor**: Monitors the export process for completion.
4. **Delete Glue Database**: Deletes the existing Glue database.
5. **Run Glue Crawler**: Runs a Glue Crawler on the exported data in S3.
6. **End**: Marks the end of the DAG.

Note: Make sure create a bucket where you want the snapshots to be and add that bucket name as TF_VAR_snapshot_bucket_name in the AWS secrets