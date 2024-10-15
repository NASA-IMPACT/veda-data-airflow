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
  source                         = "https://github.com/NASA-IMPACT/self-managed-apache-airflow/releases/download/v1.1.4/self-managed-apache-airflow.zip"
  project                        = var.project_name
  airflow_db                     = var.airflow_db
  fernet_key                     = var.fernet_key
  prefix                         = var.prefix
  private_subnets_tagname        = var.private_subnets_tagname
  public_subnets_tagname         = var.public_subnets_tagname
  vpc_id                         = var.vpc_id
  state_bucketname               = var.state_bucketname
  desired_max_workers_count      = var.workers_configuration[var.stage].max_desired_workers
  airflow_admin_password         = random_password.password.result
  airflow_admin_username         = "admin"
  rds_publicly_accessible        = var.rds_publicly_accessible
  permission_boundaries_arn      = var.permission_boundaries_arn
  custom_worker_policy_statement = var.custom_worker_policy_statement
  worker_cpu                     = var.workers_configuration[var.stage].cpu
  worker_memory                  = var.workers_configuration[var.stage].memory
  number_of_schedulers           = var.number_of_schedulers
  scheduler_cpu                  = var.scheduler_cpu
  scheduler_memory               = var.scheduler_memory
  rds_engine_version             = var.rds_configuration[var.stage].rds_engine_version
  rds_instance_class             = var.rds_configuration[var.stage].rds_instance_class
  rds_allocated_storage          = var.rds_configuration[var.stage].rds_allocated_storage
  rds_max_allocated_storage      = var.rds_configuration[var.stage].rds_max_allocated_storage
  workers_logs_retention_days    = var.workers_configuration[var.stage].workers_logs_retention_days

  extra_airflow_task_common_environment = [
    {
      name  = "AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT"
      value = "100"
    },
    {
      name  = "AIRFLOW__CORE__DEFAULT_TASK_RETRIES"
      value = var.workers_configuration[var.stage].task_retries
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
      value = "csda-airflow-data-pipeline-users"
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
    EVENT_BUCKET          = var.event_bucket
    COGNITO_APP_SECRET    = var.workflows_client_secret
    STAC_INGESTOR_API_URL = var.stac_ingestor_api_url
    Contact               = "Abdelhak"
  }
}

