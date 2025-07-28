
### Working with Airflow Variables

Airflow variables allow passing secrets, configurations, etc., to tasks without embedding sensitive values in code.
We are using AWS Secrets Manager as the secrets' backend. A secret manager will be created during the deployment with
the name <prefix>/airflow/variables/aws_dags_variables. You can add the variables there and read them in a task using
the following approach:

```python
import json
from airflow.models import Variable
var = Variable.get("aws_dags_variables")
var_json = json.loads(var)
print(var['db_secret_name'])
```

#### Working with DAG variables

If you want to use a variable in your DAG, follow these steps:

1. `Define Variables in AWS Secrets Manager:`

- Define the variables you want to use in your DAG within AWS Secrets Manager. The Secrets Manager should have a specific naming convention, where the secret name includes `${stage}`. `${stage}` is a placeholder for a stage or environment variable, indicating different environments (e.g., development, testing, production).

2. `Deployment:`

- During the deployment process, these secrets are retrieved from AWS Secrets Manager.
The retrieved secrets are then stored in a .env file.

3. `Usage in Tasks:`

- The [python-dotenv](https://pypi.org/project/python-dotenv/) library is used to access the variables stored in the .env file.
These variables can now be used within your DAG tasks.
