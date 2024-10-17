#####################################################
# Execution Role
#####################################################
resource "aws_iam_role" "lambda_exec_role" {
  provider = aws.aws_current
  name = "lambda-exec-role-s3-event-bridge-veda-${var.stage}"

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
      "Sid": ""
    }
  ]
}
EOF
}

###############################
# Logging
###############################
resource "aws_iam_policy" "lambda_logging" {
  provider = aws.aws_current
  name        = "lambda-logging-veda-${var.stage}"
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
      "Resource": "${aws_cloudwatch_log_group.group.arn}",
      "Effect": "Allow"
    }
  ]
}
EOF
}

resource "aws_iam_role_policy_attachment" "lambda_logs" {
  provider = aws.aws_current
  role       = aws_iam_role.lambda_exec_role.name
  policy_arn = aws_iam_policy.lambda_logging.arn
}

###############################
# SFN StartExecution Policy
###############################
resource "aws_iam_policy" "lambda_sfn_start_exec" {
  provider = aws.aws_current
  name        = "lambda-startexec-on-sfn-veda-${var.stage}"
  path        = "/"
  description = "IAM policy for allowing lambda to start execution on SFN"

  policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Action": [
        "states:StartExecution"
      ],
      "Resource": "arn:aws:states:us-west-1:853558080719:stateMachine:veda-data-pipelines-dev-vector-stepfunction-discover",
      "Effect": "Allow"
    }
  ]
}
EOF
}

resource "aws_iam_role_policy_attachment" "lambda_sfn_start_exec" {
  provider = aws.aws_current
  role       = aws_iam_role.lambda_exec_role.name
  policy_arn = aws_iam_policy.lambda_sfn_start_exec.arn
}

###############################
# MWAA Trigger Permissions
###############################
resource "aws_iam_policy" "lambda_trigger_mwaa_job" {
  provider = aws.aws_current
  name        = "lambda-trigger-mwaa-veda-${var.stage}"
  path        = "/"
  description = "IAM policy for allowing lambda to trigger MWAA"

  policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
      {
          "Effect": "Allow",
          "Action": "airflow:ListEnvironments",
          "Resource": "*"
      },
      {
          "Effect": "Allow",
          "Action": "airflow:*",
          "Resource": [
              "arn:aws:airflow:us-west-2:853558080719:environment/veda-pipeline-staging-mwaa",
              "arn:aws:airflow:us-west-2:853558080719:environment/veda-pipeline-dev-mwaa",
              "arn:aws:airflow:us-west-2:853558080719:environment/veda-pipeline-sit-mwaa"
          ]
      }
  ]
}
EOF
}

resource "aws_iam_role_policy_attachment" "lambda_trigger_mwaa_job" {
  provider = aws.aws_current
  role       = aws_iam_role.lambda_exec_role.name
  policy_arn = aws_iam_policy.lambda_trigger_mwaa_job.arn
}

#####################################################
# Lambda
#####################################################
data "archive_file" "archive" {
  type        = "zip"
  source_dir  = "functions/s3_event_bridge_to_sfn_execute"
  output_path = "s3_event_bridge_to_sfn_execute.zip"
}

resource "aws_lambda_function" "lambda" {
  provider = aws.aws_current
  filename         = "s3_event_bridge_to_sfn_execute.zip"
  function_name    = "s3-event-bridge-to-sfn-execute-veda-${var.stage}"
  role             = aws_iam_role.lambda_exec_role.arn
  handler          = "lambda_function.lambda_handler"
  source_code_hash = data.archive_file.archive.output_base64sha256
  runtime          = "python3.9"
  publish          = true

  environment {
    variables = {
      LOG_GROUP_NAME = "/aws/lambda/s3-event-bridge-to-sfn-execute-veda-${var.stage}"
      TARGET_MWAA_ENV = "veda-pipeline-${var.stage}-mwaa"
      TARGET_DAG_ID = "veda_discover"
      TARGET_DAG_COMMAND = "dags trigger"
    }
  }
}

resource "aws_cloudwatch_log_group" "group" {
  provider = aws.aws_current
  name              = "/aws/lambda/${aws_lambda_function.lambda.function_name}"
  retention_in_days = 5
}

#####################################################
# RESOURCE POLICY for EVENT INVOCATION
#####################################################
resource "aws_lambda_permission" "s3_invoke" {
  provider = aws.aws_current
  action           = "lambda:InvokeFunction"
  function_name    = aws_lambda_function.lambda.function_name
  principal        = "s3.amazonaws.com"
  statement_id     = "AllowInvocationFromS3Bucket-veda-${var.stage}"
  source_account   = "114506680961"
  source_arn       = "arn:aws:s3:::veda-data-store-staging"
}