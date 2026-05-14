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

module "rds_backups" {
  source                    = "./rds_backups"
  count                     = var.snapshot_bucket_name != "" ? 1 : 0
  prefix                    = var.prefix
  permission_boundaries_arn = var.permission_boundaries_arn
  snapshot_bucket_name      = var.snapshot_bucket_name
}

locals {
  airflow_dag_variables_map = merge(
    {
      EVENT_BUCKET                  = var.state_bucketname
      STAC_INGESTOR_API_URL         = var.stac_ingestor_api_url
      STAC_URL                      = var.stac_url
      VECTOR_SECRET_NAME            = var.vector_secret_name
      ASSUME_ROLE_READ_ARN          = var.assume_role_read_arn
      ASSUME_ROLE_WRITE_ARN         = var.assume_role_write_arn
      SM2A_BASE_URL                 = "https://${lower(var.subdomain)}.${var.domain_name}"
      CLOUDFRONT_TO_INVALIDATE      = var.cloudfront_to_invalidate
      CLOUDFRONT_PATH_TO_INVALIDATE = var.cloudfront_path_to_invalidate
    },
    var.snapshot_bucket_name != "" ? module.rds_backups[0].rds_backup_environment : {}
  )

  airflow_dag_variable_env_entries = [
    for k, v in local.airflow_dag_variables_map : {
      name  = "AIRFLOW_VAR_${k}"
      value = try(tostring(v), jsonencode(v))
    }
  ]
}

module "sma-base" {
  source                         = "https://github.com/NASA-IMPACT/self-managed-apache-airflow/releases/download/v1.2.0-rc1/self-managed-apache-airflow.zip"
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
  rds_deletion_protection        = var.rds_deletion_protection
  rds_storage_encrypted          = var.rds_storage_encrypted
  rds_snapshot_identifier        = var.rds_snapshot_identifier
  airflow_version                = var.airflow_version
  alb_access_logs_bucket         = var.alb_access_logs_bucket
  alb_access_logs_prefix         = var.alb_access_logs_prefix

  extra_airflow_task_common_environment = concat(
    [
      {
        name  = "AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT"
        value = "100"
      },
      {
        name  = "AIRFLOW__CORE__DEFAULT_TASK_RETRIES"
        value = var.workers_task_retries
      },
      {
        name  = "KEYCLOAK_BASE_URL"
        value = var.keycloak_base_url
      },
      {
        name  = "KEYCLOAK_REALM"
        value = var.keycloak_realm
      },
      {
        name  = "KEYCLOAK_CLIENT_ID"
        value = var.keycloak_client_id
      },
      {
        name  = "KEYCLOAK_CLIENT_SECRET"
        value = var.keycloak_client_secret
      }
    ],
    local.airflow_dag_variable_env_entries
  )
  extra_airflow_configuration = {
    keycloak_base_url      = var.keycloak_base_url
    keycloak_realm         = var.keycloak_realm
    keycloak_client_id     = var.keycloak_client_id
    keycloak_client_secret = var.keycloak_client_secret
    sm2a_base_url          = "https://${lower(var.subdomain)}.${var.domain_name}"
  }
  domain_name  = var.domain_name
  stage        = var.stage
  subdomain    = var.subdomain
  customdomain = var.customdomain
  worker_cmd   = ["airflow", "celery", "worker"]

  # Sensitive values - stored in Secrets Manager JSON blob, accessed via Variable.get("aws_dags_variables", deserialize_json=True)
  airflow_dag_secrets = {
    INGEST_API_KEYCLOAK_APP_SECRET = var.ingest_api_keycloak_client_secret
  }

  airflow_dag_variables = {}
}

resource "aws_vpc_security_group_ingress_rule" "vector_rds_ingress" {
  count             = var.vector_security_group == "null" ? 0 : 1
  security_group_id = var.vector_security_group

  from_port                    = 5432
  to_port                      = 5432
  ip_protocol                  = "tcp"
  referenced_security_group_id = module.sma-base.worker_security_group_id
}
