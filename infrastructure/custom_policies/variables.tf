variable "prefix" {}

variable "region" {}

variable "cluster_name" {}

variable "mwaa_arn" {}

variable "account_id" {}

variable "assume_role_arns" {
  type        = list(string)
  description = "Assume roles ARN (MCP)"
}
variable "cognito_app_secret" {
  type = string
}
variable "vector_secret_name" {
  type = string
}

variable "tags" {
  type = map(string)
}
