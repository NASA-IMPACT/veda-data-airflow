# VEDA-data-pipelines

This repo houses function code and deployment code for VEDA projects.

## Project layout

- [dags](./dags/) contains the Directed Acyclic Graphs which constitute Airflow state machines. This includes the python for running each task as well as the python definitions of the structure of these DAGs
- [airflow_worker/requirements](./airflow_worker/requirements.txt) contains requirements.txt file. This file is used to specify the dependencies of the workers, these libraries will be installed in all SM2A workers and can be accessed by all tasks.
- [airflow_services/requirements](./airflow_services/requirements.txt) contains requirements.txt file. This file is used to specify the dependencies of the schedulers and the webserver.
- [infrastructure](./infrastructure/) contains the terraform necessary to deploy all resources to AWS
- [scripts](./scripts/) contains bash script for deploying
-[sm2a-local-config](./sm2a-local-config) contains airflow configuration to run Airflow locally. 
Also you can define AWS credentials or other custom envs in [.env](./sm2a-local-config/env_example) file. You need
to copy ./sm2a-local-config/env_example to ./sm2a-local-config/.env and update the values of AWS secrets.

### Terraform

See [terraform-getting-started](https://developer.hashicorp.com/terraform/tutorials/aws-get-started/install-cli)

### AWS CLI

See [getting-started-install](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html)

### Docker && docker-compose
See [install-docker-and-docker-compose](https://docs.docker.com/compose/install/)

## Getting started

### Setup a local development environment

1. Initialize the metadata db (only needed once)

```shell
docker compose run --rm airflow-cli db init
```

2. Create an admin user (only needed once)

```shell
docker compose run --rm airflow-cli users create --email airflow@example.com --firstname airflow --lastname airflow --password airflow --username airflow --role Admin
```

3. Start all services

```shell
docker compose up -d
```

If you want to run the services in foreground mode, you can use the following command:
```shell

docker compose up
```

Typically, you need to initialize the database and create an admin user once. After that, you only need to start the services. <br>
After starting the services wait for a minute then visit [localhost:8080](localhost:8080).


3. Stop the services
- If you are running the services in the foreground mode, you can stop them by pressing Ctrl+C.
- If you are running the services in the background mode, you can stop them with the following command:
```bash
docker compose down
```
Note: You need to be in the same folder containing `docker-compose.yml` file


## Deployment

### Deployment via github actions

This project uses Terraform modules to deploy Apache Airflow and related AWS resources. Typically, your code will deploy automatically via Github Actions, after your Pull Request has been approved and merged. For more information about Git flow please refer to this [document](https://github.com/NASA-IMPACT/csda-data-pipelines/blob/dev/GITFLOW.md) <br>

#### Github Actions workflows layout
- [cicd.yml](./.github/workflows/cicd.yml) defines multiple jobs to:
* Check the linter
* Run unit tests
* Defines the environement where the deployment will happen
- [deploy.yml](./.github/workflows/deploy.yml) file uses OpenOIDC to obtain AWS credentials and deploys Terraform modules to AWS. The necessary environment variables are retrieved from AWS Secret Manager using the following Python [script](./scripts/generate_env_file.py).
- [gitflow.yml](./.github/workflows/gitflow.yml) provides a structured way to manage the development, testing, and deployment of terraform modules. For more info refer to [gitflow](https://github.com/NASA-IMPACT/csda-data-pipelines/blob/dev/GITFLOW.md)


### Setup a local SM2A development environment
1. Build services
```shell
make sm2a-local-build
```

2. Initialize the metadata db

```shell
make sm2a-local-init
```
🚨 NOTE: This command is typically required only once at the beginning. 
After running it, you generally do not need to run it again unless you run `make clean`,
which will require you to reinitialize SM2A with `make sm2a-local-init`

This will create an airflow username: `airflow` with password `airflow`

3. Start all services

```shell
make sm2a-local-run
```
This will start SM2A services and will be running on http://localhost:8080

4. Stop all services

```shell
make sm2a-local-stop
```

### Login to UI
To log in to the Airflow UI, you must be added to a specific GitHub team.
Contact your administrator to be added to the appropriate GitHub team that has access to the Airflow instance.
Once added, you can log in by visiting https://<domain_name> and using your GitHub credentials.

## Developers Guide

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

### Adding a DAG
The DAGs are defined in Python files located in the [dags](./dags/) directory. Each DAG should be defined as a Python module that defines a DAG object. The DAGs are scheduled by  the [Airflow Scheduler](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/scheduler.html#scheduler). Since we aim to keep the scheduler lightweight, every task-dependent library should be imported in the tasks and not at the DAG level. <br>
Example: Let's assume we need numpy library in a task, we should not import it like this
```python
from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
import numpy as np

def foo_task():
    process = ML_processing(np.rand())
    return process
```

But rather like this
```python
from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator


def foo_task():
    import numpy as np
    process = ML_processing(np.rand())
    return process
```
Doing so, the scheduler won't need numpy installed to schedule the task.

#### Working with DAG variables
If you want to use a variable in your DAG folow these steps

1- Define Variables in AWS Secrets Manager:

Define the variables you want to use in your DAG within AWS Secrets Manager.
The Secrets Manager should have a specific naming convention, with the prefix ${stage}-csda-dags-variables. Where ${stage} is a placeholder for a stage or environment variable, indicating different environments (e.g., development, testing, production).

2- Deployment:

During the deployment process, these secrets are retrieved from AWS Secrets Manager.
The retrieved secrets are then stored in a .env file.

3- Usage in Tasks:

The [python-dotenv](https://pypi.org/project/python-dotenv/) library is used to access the variables stored in the .env file.
These variables can now be used within your DAG tasks.

