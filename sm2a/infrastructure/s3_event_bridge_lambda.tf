#####################################################

#####################################################
# Execution Role
#####################################################
resource "aws_iam_role" "lambda_exec_role" {
  provider             = aws.aws_current
  name                 = "lambda-exec-role-s3-event-bridge-veda-${var.stage}"
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
resource "aws_iam_policy" "lambda_logging" {


  provider    = aws.aws_current
  name        = "sm2a-lambda-logging-veda-${var.stage}"
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

resource "aws_iam_role_policy_attachment" "lambda_logs" {


  provider   = aws.aws_current
  role       = aws_iam_role.lambda_exec_role.name
  policy_arn = aws_iam_policy.lambda_logging.arn
}


###############################
# SM2A Trigger Permissions
###############################
resource "aws_iam_policy" "lambda_trigger_sm2a_job" {


  provider    = aws.aws_current
  name        = "lambda-trigger-sm2a-veda-${var.stage}"
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

resource "aws_iam_role_policy_attachment" "lambda_trigger_sm2a_job" {


  provider   = aws.aws_current
  role       = aws_iam_role.lambda_exec_role.name
  policy_arn = aws_iam_policy.lambda_trigger_sm2a_job.arn
}

#####################################################
# Lambda
#####################################################
data "archive_file" "python_lambda_package" {


  type        = "zip"
  source_dir  = "functions/s3_event_bridge_to_sfn_execute"
  output_path = "/tmp/s3_event_bridge_to_sfn_execute.zip"
}



resource "aws_lambda_function" "lambda" {
  count = var.eis_storage_bucket_name != "null" ? 1 : 0

  provider         = aws.aws_current
  filename         = "/tmp/s3_event_bridge_to_sfn_execute.zip"
  function_name    = "s3-event-bridge-to-sm2a-dag-run-veda-${var.stage}"
  role             = aws_iam_role.lambda_exec_role.arn
  handler          = "lambda_function.lambda_handler"
  source_code_hash = data.archive_file.python_lambda_package.output_base64sha256
  runtime          = "python3.10"
  publish          = true

  environment {
    variables = {
      TARGET_DAG_ID            = var.target_dag_id
      SM2A_SECRET_MANAGER_NAME = var.sm2a_secret_manager_name
      STORAGE_BUCKET           = var.eis_storage_bucket_name
      S3_FILTER_PREFIX         = var.eis_s3_invoke_filter_prefix
    }
  }
}

resource "aws_cloudwatch_log_group" "group" {

  count = var.eis_storage_bucket_name != "null" ? 1 : 0


  provider          = aws.aws_current
  name              = "/aws/lambda/${aws_lambda_function.lambda[count.index].function_name}"
  retention_in_days = 5
}

#####################################################
# RESOURCE POLICY for EVENT INVOCATION
#####################################################

resource "aws_lambda_permission" "s3_invoke" {
  count = var.eis_storage_bucket_name != "null" ? 1 : 0

  provider      = aws.aws_current
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.lambda[count.index].function_name
  principal     = "s3.amazonaws.com"
  statement_id  = "AllowInvocationFromS3Bucket-veda-${var.stage}"
  source_arn    = "arn:aws:s3:::${var.eis_storage_bucket_name}"
}




resource "aws_s3_bucket_notification" "bucket_notification" {
  count = var.eis_storage_bucket_name != "null" ? 1 : 0
  bucket = var.eis_storage_bucket_name

  lambda_function {
    lambda_function_arn = aws_lambda_function.lambda[count.index].arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = var.eis_s3_invoke_filter_prefix
    filter_suffix       = ".gpkg"
  }

  depends_on = [aws_lambda_permission.s3_invoke]
}
