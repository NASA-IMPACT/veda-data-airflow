variable "prefix" {
}

variable "stage" {

  description = "Stage maturity (dev, sit, uat, prod...)"
}

variable "docker_image_url" {}

variable "aws_region" {}

variable "mwaa_execution_role_arn" {}

variable "mwaa_task_role_arn" {}