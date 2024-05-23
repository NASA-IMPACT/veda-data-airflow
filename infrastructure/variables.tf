
# Required variables
variable "subnet_tagname" {
  description = "Private subnet tagname to use for MWAA"
}

variable "subnet_ids" {
  type        = list(string)
  description = "Private subnets to be used for workflows api lambdas"
}

variable "vpc_id" {
  description = "Account VPC to use, this should be the same between airflow and backend"
}

variable "prefix" {
  description = "Deployment prefix"
}

variable "iam_policy_permissions_boundary_name" {
  description = "Permission boundaries"
  default     = null
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

variable "workflows_client_secret" {
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

variable "mwaa_environment_class" {
  type = map(string)
  default = {
    dev        = "mw1.small"
    staging    = "mw1.medium"
    production = "mw1.medium"
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
  default = "null"
}

variable "data_access_role_arn" {
  type = string
}

variable "raster_url" {
  type = string
}

variable "stac_url" {
  type = string
}

variable "workflow_root_path" {
  type    = string
  default = "/api/workflows"
}

variable "cloudfront_id" {
  type = string
}

variable "cognito_domain" {
  type = string
}

variable "client_id" {
  type = string
}

variable "userpool_id" {
  type = string
}

variable "workflows_lambda_policy" {
  type = list(object({
    Effect = string
    Action = list(string)
    Resource = list(string)
  }))

  default = [
      {
        Effect = "Allow",
        Action = [
          "ecr:GetDownloadUrlForLayer",
          "ecr:BatchGetImage"
        ],
        Resource = [
          "${aws_ecr_repository.workflows_api_lambda_repository.arn}",
        ],
      },
      {
        Effect = "Allow",
        Action = [
          "secretsmanager:GetSecretValue"
        ],
        Resource = [
          "arn:aws:secretsmanager:${var.aws_region}:${local.account_id}:secret:${var.cognito_app_secret}*"
        ],
      },
      {
        Action: [
          "airflow:CreateCliToken"
        ],
        Resource: [
          "arn:aws:airflow:${var.aws_region}:${local.account_id}:environment/${var.prefix}-mwaa"
        ],
        Effect: "Allow"
      },
      {
          "Effect": "Allow",
          "Action": [
            "ec2:DescribeNetworkInterfaces",
            "ec2:CreateNetworkInterface",
            "ec2:DeleteNetworkInterface",
            "ec2:DescribeInstances",
            "ec2:AttachNetworkInterface"
          ],
          "Resource": ["*"]
      }
    ]
}

variable "conditional_workflows_lambda_policy" {
  type = list(object({
    Effect = string
    Action = list(string)
    Resource = list(string)
  }))
  default = var.data_access_role_arn ? concat(var.workflows_lambda_policy, [
    {
      Effect: "Allow",
      Action: ["sts:AssumeRole"],
      Resource: [var.data_access_role_arn]
    }
  ]) : var.workflows_lambda_policy
}