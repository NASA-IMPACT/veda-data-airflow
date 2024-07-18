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

variable "custom_worker_policy_statement" {
  type = list(object({
    Effect   = string
    Action   = list(string)
    Resource = list(string)
  }))
  default = []

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


variable "rds_configuration" {
  type = object({
    dev = object({
      rds_instance_class        = string,
      rds_allocated_storage     = number,
      rds_max_allocated_storage = number,
      rds_engine_version        = string
    })
    staging = object({
      rds_instance_class        = string,
      rds_allocated_storage     = number,
      rds_max_allocated_storage = number,
      rds_engine_version        = string
    })
    prod = object({
      rds_instance_class        = string,
      rds_allocated_storage     = number,
      rds_max_allocated_storage = number,
      rds_engine_version        = string
    })

  })
  default = {
    dev = {
      rds_instance_class        = "db.t4g.medium",
      rds_allocated_storage     = 20,
      rds_max_allocated_storage = 100,
      rds_engine_version        = "13.13"
    },
    staging = {
      rds_instance_class        = "db.t4g.large",
      rds_allocated_storage     = 40,
      rds_max_allocated_storage = 100,
      rds_engine_version        = "13.13"
    },
    prod = {
      rds_instance_class        = "db.r5.xlarge",
      rds_allocated_storage     = 100,
      rds_max_allocated_storage = 200,
      rds_engine_version        = "13.13"
    }
  }
}

variable "workers_configuration" {
  type = object({
    dev = object({
      cpu                         = number,
      memory                      = number,
      max_desired_workers         = string,
      task_retries                = string,
      workers_logs_retention_days = number

    })
    staging = object({
      cpu                         = number,
      memory                      = number,
      max_desired_workers         = string,
      task_retries                = string,
      workers_logs_retention_days = number
    })
    prod = object({
      cpu                         = number,
      memory                      = number,
      max_desired_workers         = string,
      task_retries                = string,
      workers_logs_retention_days = number
    })
  })
  default = {
    dev = {
      cpu                         = 2048,
      memory                      = 4096,
      max_desired_workers         = "5"
      task_retries                = "0"
      workers_logs_retention_days = 1
    },
    staging = {
      cpu                         = 4096,
      memory                      = 8192,
      max_desired_workers         = "10",
      task_retries                = "1",
      workers_logs_retention_days = 1
    },
    prod = {
      cpu                         = 8192,
      memory                      = 16384,
      max_desired_workers         = "30",
      task_retries                = "1",
      workers_logs_retention_days = 14
    }
  }
}


variable "gh_app_client_id" {

}
variable "gh_app_client_secret" {

}
variable "gh_team_name" {

}


variable "airflow_custom_variables" {
  description = "Airflow custom variables"
  type        = map(string)
  default = {}
}

