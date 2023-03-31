resource "aws_iam_policy" "mwaa_cli_access" {
  name        = "AmazonMWAAAirflowCliAccess"
  description = "Allows mwaa environment's execution role to create cli tokens"

  policy = jsonencode({
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "airflow:CreateCliToken"
                ],
                "Resource": "${module.mwaa.mwaa_arn}"
            }
        ]
    })
}

resource "aws_iam_role_policy_attachment" "create_token_policy_attach" {
  role = "${var.prefix}-iam-role-executor"
  policy_arn = "${aws_iam_policy.mwaa_cli_access.arn}"
}