
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

variable "iam_policy_permissions_boundary_name" {
  description = "Permission boundaries"
  default = null
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
  type    = string
  default = null
}
