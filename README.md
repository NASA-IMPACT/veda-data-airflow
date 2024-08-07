# veda-data-airflow

This repo houses function code and deployment code for producing cloud-optimized
data products and STAC metadata for interfaces such as https://github.com/NASA-IMPACT/delta-ui.

## Project layout

- [dags](./dags/): Contains the Directed Acyclic Graphs which constitute Airflow state machines. This includes the python for running each task as well as the python definitions of the structure of these DAGs
- [pipeline_tasks](./dags/veda_data_pipeline/utils): Contains util functions used in python DAGs
- [data](./data/): Contains JSON files which define ingests of collections and items
- [docker_tasks](./docker_tasks/): Contains definitions tasks which we want to run in docker containers either because these tasks have special, unique dependencies or for the sake of performance (e.g. using multiprocessing)
- [infrastructure](./infrastructure/): Contains the terraform modules necessary to deploy all resources to AWS
- [custom policies](./infrastructure/custom_policies/): Contains custom policies for the mwaa environment execution role
- [scripts](./scripts/): Contains bash and python scripts useful for deploying and for running ingests

### Fetching Submodules

First time setting up the repo:
`git submodule update --init --recursive`

Afterwards:
`git submodule update --recursive --remote`

## Requirements

### Docker

See [get-docker](https://docs.docker.com/get-docker/)

### Terraform

See [terraform-getting-started](https://developer.hashicorp.com/terraform/tutorials/aws-get-started/install-cli)

### AWS CLI

See [getting-started-install](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html)

## Deployment

This project uses Terraform modules to deploy Apache Airflow and related AWS resources using Amazon's managed Airflow provider.

### Configure AWS Profile
Ensure that you have an AWS profile configured with the necessary permissions to deploy resources. The profile should be configured in the `~/.aws/credentials` file with the profile name being called `veda`, to match existing .env files.

```bash

### Make sure that environment variables are set

[`.env.example`](..env.example) contains the environment variables which are necessary to deploy. Copy this file and update its contents with actual values. The deploy script will `source` and use this file during deployment when provided through the command line:

```bash
# Copy .env.example to a new file
$cp .env.example .env
# Fill values for the environments variables

# Install the deploy requirements
$pip install -r deploy_requirements.txt

# Init terraform modules
$bash ./scripts/deploy.sh .env <<< init

# Deploy
$bash ./scripts/deploy.sh .env <<< deploy
```

### Fetch environment variables using AWS CLI

To retrieve the variables for a stage that has been previously deployed, the secrets manager can be used to quickly populate an .env file with [`scripts/sync-env-local.sh`](scripts/sync-env-local.sh). 

```
./scripts/sync-env-local.sh <app-secret-name>
```

> [!IMPORTANT] 
> Be careful not to check in `.env` (or whatever you called your env file) when committing work.

Currently, the client id and domain of an existing Cognito user pool programmatic client must be supplied in [configuration](ingest_api/infrastructure/config.py) as `VEDA_CLIENT_ID` and `VEDA_COGNITO_DOMAIN` (the [veda-auth project](https://github.com/NASA-IMPACT/veda-auth) can be used to deploy a Cognito user pool and client). To dispense auth tokens via the workflows API swagger docs, an administrator must add the ingest API lambda URL to the allowed callbacks of the Cognito client.

# Gitflow Model
[VEDA pipeline gitflow](./GITFLOW.md)
# License
This project is licensed under **Apache 2**, see the [LICENSE](LICENSE) file for more details.

