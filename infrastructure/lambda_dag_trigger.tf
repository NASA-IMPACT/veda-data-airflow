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
