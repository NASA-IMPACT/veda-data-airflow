locals {
  workflows_lambda_policy = [
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
      Action : [
        "airflow:CreateCliToken"
      ],
      Resource : [
        "arn:aws:airflow:${var.aws_region}:${local.account_id}:environment/${var.prefix}-mwaa"
      ],
      Effect : "Allow"
    },
    {
      "Effect" : "Allow",
      "Action" : [
        "ec2:DescribeNetworkInterfaces",
        "ec2:CreateNetworkInterface",
        "ec2:DeleteNetworkInterface",
        "ec2:DescribeInstances",
        "ec2:AttachNetworkInterface"
      ],
      "Resource" : ["*"]
    }
  ]

  conditional_workflows_lambda_policy = var.data_access_role_arn != "" ? concat(local.workflows_lambda_policy, [
    {
      Effect : "Allow",
      Action : ["sts:AssumeRole"],
      Resource : [var.data_access_role_arn]
    }
  ]) : local.workflows_lambda_policy

  build_jwks_url = format("https://cognito-idp.%s.amazonaws.com/%s/.well-known/jwks.json", local.aws_region, var.userpool_id)
}
