# VEDA SM2A (Self-Managed Apache Airflow)

This repo houses function code and deployment code for VEDA projects.

## Project layout

- [dags](./dags/) contains the Directed Acyclic Graphs which constitute Airflow state machines. This includes the Python for running each task as well as the Python definitions of the structure of these DAGs. Files that define a DAG object are considered top-level DAG files, which are processed by Airflow.
- [groups](./dags/veda_data_pipeline/groups/) contain common groups of tasks that can be reused in multiple DAGs.
- [utils](./dags/veda_data_pipeline/utils) contains shared low-level functions used in tasks and DAGs.
- [airflow_worker](./airflow_worker/) contains a dockerfile, as well as requirements for Airflow workers. This service runs the tasks in the DAGs.
- [airflow_services](./airflow_services/) contains a dockerfile, as well as requirements for the Airflow scheduler, DAG processor and the webserver. These requirements are largely dependent on the Airflow version, as well as any requirements needed to parse the top-level DAG code.
- [infrastructure](./infrastructure/) contains the Terraform necessary to deploy all resources to AWS
- [scripts](./scripts/) contains bash scripts for deploying and operating Airflow instances.
- [sm2a-local-config](./sm2a-local-config) contains airflow configuration to run Airflow locally.

## Getting started

### Terraform

See [terraform-getting-started](https://developer.hashicorp.com/terraform/tutorials/aws-get-started/install-cli)

### AWS CLI

See [getting-started-install](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html)

### Docker && docker-compose

See [install-docker-and-docker-compose](https://docs.docker.com/compose/install/)

### Python

- Install [uv](https://docs.astral.sh/uv/getting-started/installation/).
- Run `uv sync` to install the required python packages. By default, all optional dependencies are included. To avoid this, use `uv sync --no-default-groups`.

### Configuration
- ⚠️ You need to copy ./sm2a/sm2a-local-config/env_example to ./sm2a/sm2a-local-config/.env
- You can define AWS credentials or other custom envs in [.env](./sm2a/sm2a-local-config/.env) file.
- ⚠️  If you update ./sm2a/sm2a-local-config/.env file you should run `make sm2a-local-run` again

### Fetch environment variables using AWS CLI

To retrieve the variables for a stage that has been previously deployed, the secrets manager can be used to quickly populate an .env file with [`scripts/sync-env-local.sh`](scripts/sync-env-local.sh).

```
./scripts/sync-env-local.sh <app-secret-name>
```

> [!IMPORTANT]
> Be careful not to check in `.env` (or whatever you called your env file) when committing work.

Currently, the client id and domain of an existing Cognito user pool programmatic client must be supplied in [configuration](ingest_api/infrastructure/config.py) as `VEDA_CLIENT_ID` and `VEDA_COGNITO_DOMAIN` (the [veda-auth project](https://github.com/NASA-IMPACT/veda-auth) can be used to deploy a Cognito user pool and client). To dispense auth tokens via the workflows API swagger docs, an administrator must add the ingest API lambda URL to the allowed callbacks of the Cognito client.


### Setup a local SM2A development environment

1. Build services

```shell
make sm2a-local-build
```

2. Initialize the metadata db

> [!NOTE]
> This command is typically required only once at the beginning.
After running it, you generally do not need to run it again unless you run `make clean`,
which will require you to reinitialize SM2A with `make sm2a-local-init`

```shell
make sm2a-local-init
```

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

## Deployment

### Deployment via GitHub Actions

This project uses Terraform modules to deploy Apache Airflow and related AWS resources. Typically, your code will deploy automatically via Github Actions, after your Pull Request has been approved and merged.

#### Github Actions workflows layout

- [cicd.yml](./.github/workflows/cicd.yml) defines multiple jobs to:
  - Check the linter
  - Run unit tests
  - Define the environment where the deployment will happen
- [deploy.yml](./.github/workflows/deploy.yml) file uses OpenOIDC to obtain AWS credentials and deploys Terraform modules to AWS. The necessary environment variables are retrieved from AWS Secret Manager using the following Python [script](./scripts/generate_env_file.py).
- [gitflow.yml](./.github/workflows/gitflow.yml) provides a structured way to manage the development, testing, and deployment of terraform modules. For more info refer to [gitflow](https://github.com/NASA-IMPACT/csda-data-pipelines/blob/dev/GITFLOW.md)


### Login to UI

To log in to the Airflow UI, you must be added to a specific GitHub team, depending on the deployed Airflow configuration.
Contact your instance administrator to be added to the appropriate GitHub team that has access to the Airflow instance.
Once added, you can log in by visiting https://<domain_name> and using your GitHub credentials.

## Developers Guide

Please review the [docs](docs/) folder for more information on how to create a DAG, add tasks, and manage dependencies and variables.


### Adding a DAG

The DAGs are defined in Python files located in the [dags](./dags/) directory. Each DAG should be defined as a Python module that defines a DAG object. The DAGs are scheduled by  the [Airflow Scheduler](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/scheduler.html#scheduler). Since we aim to keep the scheduler lightweight, every task-dependent library should be imported in the tasks and not at the DAG level.
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

## DAG Launcher Role Overview
The DAG Launcher role in Airflow is designed to provide users with the necessary permissions to manage and launch DAGs
in the Airflow UI. This role allows users to perform actions such as reading DAG runs,
and interacting with various views like Task Instances, Jobs, and XComs.

The permissions granted are tailored to streamline the interaction with the DAG management interface,
enabling effective monitoring and control over DAG executions.

### Key Permissions Assigned to the DAG Launcher Role:
- Permission on Dag runs for `veda_discover`, `veda_dataset_pipeline`, `veda_collection_pipeline`
- Read access to "My Profile", "DAG Runs", "Jobs", "Task Instances", "XComs", "DAG Dependencies", "Task Logs", and "Website".
- Create, Read, Edit, and Menu Access for DAG runs and DAG-related views (e.g., DAGs, Documentation).
- Edit access to specific DAGs like veda_discover.
- Integrating GitHub Users with the Airflow DAG UI
To grant users access to the Airflow UI, including the ability to manage DAGs and view the Swagger interface, 
GitHub users must be added to a GitHub [team](https://github.com/orgs/NASA-IMPACT/teams/veda-dag-launcher).

