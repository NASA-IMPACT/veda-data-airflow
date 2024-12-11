terraform {
  required_providers {
    aws = {
      version = "~> 4.0"
    }
  }
  required_version = ">= 1.3"
}

provider "aws" {
  region = var.aws_region
}
resource "random_password" "password" {
  length           = 8
  special          = true
  override_special = "_%@"
}



module "sma-base" {
  source                         = "https://github.com/NASA-IMPACT/self-managed-apache-airflow/releases/download/v1.1.5/self-managed-apache-airflow.zip"
  project                        = var.project_name
  airflow_db                     = var.airflow_db
  fernet_key                     = var.fernet_key
  prefix                         = var.prefix
  private_subnets_tagname        = var.private_subnets_tagname
  public_subnets_tagname         = var.public_subnets_tagname
  vpc_id                         = var.vpc_id
  state_bucketname               = var.state_bucketname
  desired_max_workers_count      = var.desired_max_workers_count
  airflow_admin_password         = random_password.password.result
  airflow_admin_username         = "admin"
  rds_publicly_accessible        = var.rds_publicly_accessible
  permission_boundaries_arn      = var.permission_boundaries_arn
  custom_worker_policy_statement = var.custom_worker_policy_statement
  worker_cpu                     = tonumber(var.workers_cpu)
  worker_memory                  = tonumber(var.workers_memory)
  number_of_schedulers           = var.number_of_schedulers
  scheduler_cpu                  = tonumber(var.scheduler_cpu)
  scheduler_memory               = tonumber(var.scheduler_memory)
  rds_engine_version             = var.rds_engine_version
  rds_instance_class             = var.rds_instance_class
  rds_allocated_storage          = tonumber(var.rds_allocated_storage)
  rds_max_allocated_storage      = tonumber(var.rds_max_allocated_storage)
  workers_logs_retention_days    = tonumber(var.workers_logs_retention_days)

  extra_airflow_task_common_environment = [
    {
      name  = "AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT"
      value = "100"
    },
    {
      name  = "AIRFLOW__CORE__DEFAULT_TASK_RETRIES"
      value = var.workers_task_retries
    },
    {
      name  = "GH_CLIENT_ID"
      value = var.gh_app_client_id
    },
    {
      name  = "GH_CLIENT_SECRET"
      value = var.gh_app_client_secret
    },
    {
      name  = "GH_ADMIN_TEAM_ID"
      value = var.gh_team_name
    },
    {
      name  = "GH_USER_TEAM_ID"
      value = var.gh_user_team_id
    }


  ]
  extra_airflow_configuration = {
    gh_app_client_id     = var.gh_app_client_id
    gh_app_client_secret = var.gh_app_client_secret
    gh_team_id           = var.gh_team_name
  }
  domain_name = var.domain_name
  stage       = var.stage
  subdomain   = var.subdomain
  worker_cmd  = ["/home/airflow/.local/bin/airflow", "celery", "worker"]

  airflow_custom_variables = {
    EVENT_BUCKET          = var.state_bucketname
    COGNITO_APP_SECRET    = var.workflows_client_secret
    STAC_INGESTOR_API_URL = var.stac_ingestor_api_url
    STAC_URL              = var.stac_url
    VECTOR_SECRET_NAME    = var.vector_secret_name
    ASSUME_ROLE_READ_ARN = var.assume_role_read_arn
    ASSUME_ROLE_WRITE_ARN = var.assume_role_write_arn
  }
}

