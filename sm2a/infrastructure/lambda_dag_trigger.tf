#####################################################

#####################################################
# Execution Role
#####################################################


resource "aws_iam_role" "lambda_dag_trigger_exec_role" {
  provider             = aws.aws_current
  name                 = "${var.prefix}-${var.lambda_dag_trigger_function_name}"
  permissions_boundary = var.permission_boundaries_arn

  assume_role_policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Action": "sts:AssumeRole",
      "Principal": {
        "Service": "lambda.amazonaws.com"
      },
      "Effect": "Allow",
      "Sid": "lambdaassumerole"
    }
  ]
}
EOF
}

###############################
# Logging
###############################


resource "aws_iam_policy" "lambda_dag_trigger_logging" {


  provider    = aws.aws_current
  name        = "${var.prefix}-${var.lambda_dag_trigger_function_name}-policy"
  path        = "/"
  description = "IAM policy for logging from a lambda"

  policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Action": [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ],
      "Resource": "arn:aws:logs:${local.aws_region}:${local.account_id}:*",
      "Effect": "Allow"
    }
  ]
}
EOF
}

resource "aws_iam_role_policy_attachment" "lambda_dag_trigger_logs" {


  provider   = aws.aws_current
  role       = aws_iam_role.lambda_dag_trigger_exec_role.name
  policy_arn = aws_iam_policy.lambda_dag_trigger_logging.arn
}


###############################
# SM2A Trigger Permissions
###############################
resource "aws_iam_policy" "lambda_dag_trigger_sm2a" {


  provider    = aws.aws_current
  name        = "${var.prefix}-${var.lambda_dag_trigger_function_name}-dag-trigger"
  path        = "/"
  description = "IAM policy for allowing lambda to trigger SM2A"

  policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
      {
          "Effect": "Allow",
          "Action": [
            "secretsmanager:GetSecretValue",
            "secretsmanager:DescribeSecret"
            ],
          "Resource": "arn:aws:secretsmanager:${local.aws_region}:${local.account_id}:secret:sm2a-*"
      }
  ]
}
EOF
}

resource "aws_iam_role_policy_attachment" "lambda_dag_trigger_sm2a" {


  provider   = aws.aws_current
  role       = aws_iam_role.lambda_dag_trigger_exec_role.name
  policy_arn = aws_iam_policy.lambda_dag_trigger_sm2a.arn
}

#####################################################
# Lambda
#####################################################
data "archive_file" "python_dag_trigger_lambda_package" {


  type        = "zip"
  source_dir  = "functions/trigger_sm2a_dag"
  output_path = "/tmp/trigger_sm2a_dag.zip"
}



resource "aws_lambda_function" "dag_trigger_lambda" {

  provider         = aws.aws_current
  filename         = "/tmp/trigger_sm2a_dag.zip"
  function_name    = "${var.prefix}-${var.lambda_dag_trigger_function_name}"
  role             = aws_iam_role.lambda_dag_trigger_exec_role.arn
  handler          = "lambda_function.lambda_handler"
  source_code_hash = data.archive_file.python_lambda_package.output_base64sha256
  runtime          = "python3.10"
  publish          = true

  environment {
    variables = {
      SM2A_SECRET_MANAGER_NAME = var.sm2a_secret_manager_name

    }
  }
}

resource "aws_cloudwatch_log_group" "dag_trigger_lambda_log_group" {


  provider          = aws.aws_current
  name              = "/aws/lambda/${aws_lambda_function.dag_trigger_lambda.function_name}"
  retention_in_days = 5
}
