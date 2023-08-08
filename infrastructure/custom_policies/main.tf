
data "aws_iam_policy_document" "mwaa_executor_policies" {
  statement {
    effect = "Allow"
    actions = [
      "ecs:RunTask",
      "ecs:StopTask",
      "ecs:DescribeTasks",
      "ecs:RegisterTaskDefinition",
      "ecs:DescribeTaskDefinition",
      "ecs:DeregisterTaskDefinition"
    ]
    resources = [
      "*"
    ]
  }


  statement {
    effect = "Allow"
    actions = [
      "logs:CreateLogStream",
      "logs:CreateLogGroup",
      "logs:PutLogEvents",
      "logs:GetLogEvents",
      "logs:GetLogRecord",
      "logs:GetLogGroupFields",
      "logs:GetQueryResults"
    ]
    resources = [
      "*"
    ]
  }

  statement {
    effect = "Allow"
    actions = [
      "execute-api:Invoke"
    ]
    resources = ["arn:aws:execute-api:${var.region}:${var.account_id}:*"]
  }
  statement {
    effect = "Allow"
    actions = [
      "ecr:GetAuthorizationToken",
      "ecr:BatchCheckLayerAvailability",
      "ecr:GetDownloadUrlForLayer",
      "ecr:BatchGetImage",
      "iam:PassRole"
    ]
    resources = ["*"]
  }

  statement {
    effect = "Allow"
    actions = [
      "secretsmanager:GetSecretValue",
      "secretsmanager:DescribeSecret"
    ]
    resources = [
      "arn:aws:secretsmanager:${var.region}:${var.account_id}:secret:${var.cognito_app_secret}-??????",
      "arn:aws:secretsmanager:${var.region}:${var.account_id}:secret:${var.vector_secret_name}-??????"
    ]
  }

  statement {
    effect = length(var.assume_role_arns) > 0 ? "Allow" : "Deny"
    actions = [
      "sts:AssumeRole"
    ]
    resources = length(var.assume_role_arns) > 0 ? var.assume_role_arns: ["*"]
  }

  statement {
    effect = "Allow"
    actions = [
      "s3:GetObject*",
      "s3:GetBucket*",
      "s3:List*",
      "s3:Copy*",
      "s3:Put*",
    ]
    resources = [
      "arn:aws:s3:::ghgc-*",
      "arn:aws:s3:::ghgc-*/*"
    ]
  }

  statement {
    effect = "Allow"
    actions = [
      "s3:GetObject*",
      "s3:GetBucket*",
      "s3:List*"
    ]
    resources = [
      "*",
    ]
  }

  statement {
    effect    = "Allow"
    actions   = ["airflow:CreateCliToken"]
    resources = [var.mwaa_arn]
  }
}


resource "aws_iam_policy" "read_data" {
  name        = "${var.prefix}_task_executor"
  path        = "/"
  description = "Use docker images as airflow tasks"
  policy      = data.aws_iam_policy_document.mwaa_executor_policies.json
}






