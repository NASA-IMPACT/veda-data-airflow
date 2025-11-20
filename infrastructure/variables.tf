variable "airflow_db" {
  type = object({
    db_name  = string
    username = string
    password = string
    port     = number
  })
  sensitive = true
}

variable "aws_region" {
  default = "us-west-2"
}


variable "prefix" {
}

variable "fernet_key" {
}


variable "vpc_id" {
}
variable "private_subnets_tagname" {

}
variable "public_subnets_tagname" {

}
variable "state_bucketname" {

}

variable "permission_boundaries_arn" {
  default = "null"
}

variable "rds_publicly_accessible" {
  default = false
}


variable "scheduler_cpu" {
  type    = number
  default = 1024 * 2
}
variable "scheduler_memory" {
  type    = number
  default = 2048 * 2
}

variable "number_of_schedulers" {
  default = 1
}

variable "domain_name" {

}
variable "stage" {
  default = "dev"
}

variable "subdomain" {
  default = "null"
}

variable "desired_max_workers_count" {
  default = "5"
}

variable "gh_app_client_id" {

}
variable "gh_app_client_secret" {

}
variable "gh_team_name" {

}

variable "custom_worker_policy_statement" {
  type = list(object({
    Effect   = string
    Action   = list(string)
    Resource = list(string)
  }))
  default = [
    {
      Effect = "Allow"
      Action = [
        "sts:AssumeRole",
        "iam:PassRole",
        "logs:GetLogEvents"
      ]
      "Resource" : [
        "*"
      ]

    },
        {
      Sid    = "VEDA-RDS-Disaster-Recovery"
      Effect = "Allow"
      Action = [
        "rds:Describe*",
        "rds:Start*",
        "kms:*",
        "glue:Get*",
        "glue:CreateCrawler",
        "glue:StartCrawler",
        "glue:UpdateCrawler"
      ]
      Resource = [
        "*"
      ]
    },
    {
      "Effect" : "Allow",
      "Action" : [
        "glue:DeleteDatabase"
      ],
      "Resource" : [
        "arn:aws:glue:us-west-2:*:catalog",
        "arn:aws:glue:us-west-2:*:database/*",
        "arn:aws:glue:us-west-2:*:table/*",
        "arn:aws:glue:us-west-2:*:userDefinedFunction/*"
      ]
    },
    {
            "Effect": "Allow",
            "Action": ["cloudfront:CreateInvalidation"],
            "Resource": ["arn:aws:cloudfront::*:distribution/*"]
    },
    {
      Effect = "Allow"
      Action = [
        "secretsmanager:GetSecretValue"
      ]
      Resource = [
        "arn:aws:secretsmanager:us-west-2:853558080719:secret:veda-keycloak*",
        "arn:aws:secretsmanager:us-west-2:114506680961:secret:veda-keycloak*"
      ]
    },
    {
      Effect = "Allow"
      Action = [
        "kms:Decrypt"
      ]
      Resource = [
        "arn:aws:kms:us-west-2:853558080719:key/360b41aa-0c14-4d64-a213-fb7c7ac83cd7"
      ]
    }
  ]

}

variable "project_name" {
  type    = string
  default = "SM2A"
}


variable "gh_user_team_id" {
  default = "csda-airflow-data-pipeline-users"
}

variable "workflows_client_secret" {
}
variable "stac_ingestor_api_url" {
}

variable "stac_url" {
}

variable "vector_secret_name" {
  type    = string
  default = "null"
}

variable "vector_security_group" {
  type = string
  default = "null"
}

variable "sm2a_secret_manager_name" {
  type    = string
  default = "null"
}

variable "workers_cpu" {
  default = 2048
}
variable "workers_memory" {
  default = 4096
}

variable "rds_engine_version" {
  default = "13"
}
variable "rds_instance_class" {
  default = "db.t4g.medium"
}
variable "rds_allocated_storage" {
  default = 20
}
variable "rds_max_allocated_storage" {
  default = 200
}
variable "workers_logs_retention_days" {
  default = 1
}

variable "workers_task_retries" {
  default = "1"
}

variable "assume_role_read_arn" {
  type    = string
  default = ""
}

variable "assume_role_write_arn" {
  type    = string
  default = ""
}

variable "gh_dag_launcher_team_id" {
  default = "VEDA-DAG-Launcher"
}

variable "snapshot_bucket_name" {
  default = ""
}
variable "snapshot_export_role" {
  default = ""
}
variable "glue_role_arn" {
  default = ""
}
variable "s3_export_kms_key_id" {
  default = ""
}

variable "cloudfront_to_invalidate" {
  default = null
}
variable "cloudfront_path_to_invalidate" {
  default = null

}
variable "lambda_dag_trigger_function_name" {
  default = "trigger-sm2a-dag"
}

variable ingest_api_keycloak_client_secret {
 type = string
}

variable "airflow_version" {
  type    = string
  default = "2.10.5"
}

variable "rds_deletion_protection" {
  description = "Enable deletion protection on Airflow RDS instance"
  type        = bool
  default     = true
}

variable "rds_storage_encrypted" {
  description = "Encrypt Airflow RDS data at rest"
  type        = bool
}

variable "rds_snapshot_identifier" {
  description = "Snapshot from which to create Airflow RDS instance"
  default     = null
}
