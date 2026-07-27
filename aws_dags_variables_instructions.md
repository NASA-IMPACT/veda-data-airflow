# AWS DAGs Variables Configuration

This JSON file contains the configuration variables that Airflow needs to run the VEDA data pipelines.

## How to Use

### Option 1: Upload to AWS Secrets Manager (Recommended for Production)
```bash
aws secretsmanager create-secret \
  --name veda-pipeline-dev/airflow/variables/aws_dags_variables \
  --secret-string file://aws_dags_variables.json \
  --region us-west-2
```

Or update an existing secret:
```bash
aws secretsmanager update-secret \
  --secret-id veda-pipeline-dev/airflow/variables/aws_dags_variables \
  --secret-string file://aws_dags_variables.json \
  --region us-west-2
```

### Option 2: Add via Airflow UI (Quick Fix for Development)
1. Navigate to Airflow UI → Admin → Variables
2. Click "+" to add a new variable
3. Key: `aws_dags_variables`
4. Value: Paste the JSON content from `aws_dags_variables.json`
5. Click Save

## Required Fields to Update

Before using this file, you MUST update the following placeholder values:

### 1. INGEST_API_KEYCLOAK_APP_SECRET
- **Description**: Keycloak client secret for authenticating with the STAC ingestor API
- **How to get it**: Contact your Keycloak administrator or retrieve from AWS Secrets Manager
- **Example**: `"a1b2c3d4-e5f6-7g8h-9i0j-k1l2m3n4o5p6"`

### 2. ASSUME_ROLE_READ_ARN
- **Description**: IAM Role ARN that Airflow workers assume to read data from S3
- **Format**: `arn:aws:iam::ACCOUNT_ID:role/ROLE_NAME`
- **Example**: `"arn:aws:iam::114506680961:role/veda-data-access-role-dev"`

### 3. ASSUME_ROLE_WRITE_ARN
- **Description**: IAM Role ARN that Airflow workers assume to write data to S3
- **Format**: `arn:aws:iam::ACCOUNT_ID:role/ROLE_NAME`
- **Example**: `"arn:aws:iam::114506680961:role/veda-data-write-role-dev"`

## Pre-configured Fields (Disasters-specific)

These fields are already configured for the disasters environment:

- **EVENT_BUCKET**: `veda-tf-state-shared-disasters` - S3 bucket for events and processing data
- **STAC_INGESTOR_API_URL**: `https://dev.disasters.openveda.cloud/api/ingest/` - Disasters STAC ingestor endpoint
- **STAC_URL**: `https://dev.disasters.openveda.cloud/api/stac` - Public STAC catalog endpoint
- **SM2A_BASE_URL**: `https://airflow.disasters.openveda.cloud` - Airflow base URL

## Optional Fields

These fields are set to `null` or default values and may not be needed:

- **VECTOR_SECRET_NAME**: AWS Secrets Manager secret name for vector database credentials
- **CLOUDFRONT_TO_INVALIDATE**: CloudFront distribution ID to invalidate after updates
- **CLOUDFRONT_PATH_TO_INVALIDATE**: CloudFront path pattern to invalidate

## Terraform Integration

This configuration matches the terraform variables in `infrastructure/terraform.tfvars`. 
When deployed via Terraform, these values are automatically synced to AWS Secrets Manager.

## Verification

After updating the Airflow variable, verify it's accessible:

```python
from airflow.models.variable import Variable
import json

vars = Variable.get("aws_dags_variables", deserialize_json=True)
print(vars["STAC_INGESTOR_API_URL"])  # Should print: https://dev.disasters.openveda.cloud/api/ingest/
```
