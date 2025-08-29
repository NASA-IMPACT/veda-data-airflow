# Lambda DAG Trigger
This AWS Lambda function is designed to trigger an Apache Airflow DAG run through a direct HTTP API
call using credentials securely stored in AWS Secrets Manager.

## 🧩 Features

- Authenticates with the Airflow webserver using Basic Auth
- Dynamically triggers a specified DAG with optional runtime configuration
- Generates a unique dag_run_id for each invocation
- Designed to be invoked via an event payload (e.g., AWS CLI, API Gateway, Step Function)

## ⚙️ Environment Variables
| Variable Name              | Description                                                                        |
|---------------------------|------------------------------------------------------------------------------------|
| `SM2A_SECRET_MANAGER_NAME`| Name of the AWS Secrets Manager secret that stores Airflow credentials and the webserver URL |

## 🔐 Secrets Manager Format
The secret stored in AWS Secrets Manager should have the following JSON structure:

```json
{
  "airflow_webserver_url": "your-airflow-domain.com",
  "airflow_admin_username": "your-username",
  "airflow_admin_password": "your-password"
}
```

## 🚀 Lambda Function Input Format
When invoking the function (e.g., via AWS CLI or API Gateway), provide the following JSON payload:

```json
{
  "payload": {
    "dag_name": "example_dag",
    "conf": {
      "key1": "value1",
      "key2": "value2"
    }
  }
}
```

```bash
aws lambda invoke \
  --function-name sm2a-dev-trigger-sm2a-dag \
  --payload '{"payload": {"dag_name": "example_etl_flow_test", "conf": {"key1": "value1", "key2": "value2"}}}' \
  response.json
{
    "StatusCode": 200,
    "ExecutedVersion": "$LATEST"
}

```

- dag_name (required): The ID of the DAG you want to trigger

- conf (optional): Dictionary passed as runtime configuration to the DAG

## ✅ Output
The function returns a response containing:

- statusCode: HTTP status of the DAG trigger request

- body: Response body from the Airflow API

Example:

```json
{
  "statusCode": 200,
  "body": "{\"dag_run_id\": \"example_dag-uuid\", ... }"
}
```


## 📌 Notes

- This function uses Basic Authentication—ensure you use HTTPS and store credentials securely.
- The function expects the Airflow webserver to be publicly accessible or accessible via a VPC if the Lambda is running in one.
- The credentials used are expected to have the permission to execute the DAG
