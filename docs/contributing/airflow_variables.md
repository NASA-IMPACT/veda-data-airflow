
# Working with Airflow Variables

Airflow variables are used to pass configuration and secrets to DAG tasks. There are two types of variables, depending on sensitivity:

## Non-sensitive variables (environment variables)

Non-sensitive configuration values (bucket names, URLs, ARNs) are set as `AIRFLOW_VAR_*` environment variables on the ECS containers via the `airflow_dag_variables` Terraform input. Airflow resolves these automatically when you call `Variable.get()`.

```python
from airflow.models.variable import Variable

bucket = Variable.get("EVENT_BUCKET")
stac_url = Variable.get("STAC_URL")
```

These variables are defined in `infrastructure/main.tf` under the `airflow_dag_variables` block. To add a new non-sensitive variable:

1. Add the Terraform variable to `infrastructure/variables.tf`
2. Add the mapping in `infrastructure/main.tf` under `airflow_dag_variables`
3. Access it in your DAG with `Variable.get("YOUR_VARIABLE_NAME")`

## Sensitive variables (Secrets Manager)

Sensitive values (API secrets, credentials) are stored as a JSON blob in AWS Secrets Manager under `<prefix>/airflow/variables/aws_dags_variables`, configured via the `airflow_dag_secrets` Terraform input.

```python
from airflow.models.variable import Variable

secrets = Variable.get("aws_dags_variables", deserialize_json=True)
app_secret = secrets.get("INGEST_API_KEYCLOAK_APP_SECRET")
```

To add a new sensitive variable:

1. Add the Terraform variable to `infrastructure/variables.tf` (mark as `sensitive = true`)
2. Add the mapping in `infrastructure/main.tf` under `airflow_dag_secrets`
3. Access it in your DAG by deserializing the `aws_dags_variables` JSON blob

## Current variable inventory

**Non-sensitive (`airflow_dag_variables` -> `AIRFLOW_VAR_*` env vars):**

- `EVENT_BUCKET` - S3 bucket for pipeline events
- `STAC_INGESTOR_API_URL` - STAC ingestor API endpoint
- `STAC_URL` - STAC catalog URL
- `VECTOR_SECRET_NAME` - Name of the vector DB secret in Secrets Manager
- `ASSUME_ROLE_READ_ARN` - IAM role ARN for read access
- `ASSUME_ROLE_WRITE_ARN` - IAM role ARN for write access
- `SM2A_BASE_URL` - SM2A application URL
- `CLOUDFRONT_TO_INVALIDATE` - CloudFront distribution ID for cache invalidation
- `CLOUDFRONT_PATH_TO_INVALIDATE` - CloudFront path pattern for invalidation
- `SNAPSHOT_BUCKET_NAME` - S3 bucket for RDS snapshots (conditional)
- `GLUE_ROLE_ARN` - IAM role ARN for Glue crawlers (conditional)
- `S3_EXPORT_ROLE_ARN` - IAM role ARN for RDS S3 export (conditional)
- `S3_EXPORT_KMS_KEY_ID` - KMS key ID for RDS S3 export (conditional)

**Sensitive (`airflow_dag_secrets` -> Secrets Manager JSON blob):**

- `INGEST_API_KEYCLOAK_APP_SECRET` - Keycloak client secret for ingest API authentication

## Testing

In tests, non-sensitive variables are set as individual `AIRFLOW_VAR_*` environment variables in `tests/conftest.py`. Sensitive variables remain in the `AIRFLOW_VAR_AWS_DAGS_VARIABLES` JSON blob.
