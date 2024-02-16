
# Required variables
variable "subnet_tagname" {
  description = "Private subnet tagname to use for MWAA"
}
variable "vpc_id" {
  description = "Account VPC to use"
}

variable "prefix" {
  description = "Deployment prefix"
}

variable "iam_role_permissions_boundary" {
  description = "Permission boundaries"
}

variable "assume_role_arns" {
  type        = list(string)
  description = "Assume role ARNs (MCP)"
}
# Optional variables

variable "aws_profile" {
  description = "AWS profile"
  default     = null
}
variable "aws_region" {
  default = "us-west-2"
}

variable "stage" {
  default = "dev"
}

variable "cognito_app_secret" {
  type = string
}

variable "jwks_url" {
  type = string
}

variable "cognito_userpool_id" {
  type = string
}

variable "cognito_client_id" {
  type = string
}

variable "cognito_client_secret" {
  type = string
}

variable "stac_ingestor_api_url" {
  type = string
}
variable "min_workers" {
  type = map(number)
  default = {
    dev        = 2
    staging    = 3
    production = 3
  }
}
variable "vector_secret_name" {
  type = string
}
variable "vector_security_group" {
  type = string
}
variable "vector_vpc" {
  type = string
}

variable "data_access_role_arn" {
  type = string
}

variable "workflow_root_path" {
  type = string
  default = "/api/workflows"
}

variable "ingest_url" {
  type = string
}

variable "raster_url" {
  type = string
}

variable "stac_url" {
  type = string
}

variable "cloudfront_id" {
  type = string
}

