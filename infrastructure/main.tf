module "mwaa" {
  source                           = "https://github.com/NASA-IMPACT/mwaa_tf_module/releases/download/v1.1.9/mwaa_tf_module.zip"
  prefix                           = var.prefix
  vpc_id                           = var.vpc_id
  iam_role_additional_arn_policies = merge(module.custom_policy.custom_policy_arns_map)
  permissions_boundary_arn         = var.iam_policy_permissions_boundary_name == "null" ? null : "arn:aws:iam::${local.account_id}:policy/${var.iam_policy_permissions_boundary_name}"
  subnet_tagname                   = var.subnet_tagname
  local_requirement_file_path      = "${path.module}/../dags/requirements.txt"
  local_dag_folder                 = "${path.module}/../dags/"
  mwaa_variables_json_file_id_path = { file_path = local_file.mwaa_variables.filename, file_id = local_file.mwaa_variables.id }
  stage                            = var.stage
  airflow_version                  = "2.4.3"
  environment_class                = lookup(var.mwaa_environment_class, var.stage, "mw1.small")
  min_workers                      = lookup(var.min_workers, var.stage, 1)
  ecs_containers = [
    {
      handler_file_path         = "${path.module}/../docker_tasks/build_stac/handler.py"
      docker_file_path          = "${path.module}/../docker_tasks/build_stac/Dockerfile"
      ecs_container_folder_path = "${path.module}/../docker_tasks/build_stac"
      ecr_repo_name             = "${var.prefix}-veda-build_stac"
    },
    {
      handler_file_path         = "${path.module}/../docker_tasks/cogify_transfer/handler.py"
      docker_file_path          = "${path.module}/../docker_tasks/cogify_transfer/Dockerfile"
      ecs_container_folder_path = "${path.module}/../docker_tasks/cogify_transfer"
      ecr_repo_name             = "${var.prefix}-veda-cogify_transfer"
    },
    {
      handler_file_path         = "${path.module}/../docker_tasks/vector_ingest/handler.py"
      docker_file_path          = "${path.module}/../docker_tasks/vector_ingest/Dockerfile"
      ecs_container_folder_path = "${path.module}/../docker_tasks/vector_ingest"
      ecr_repo_name             = "${var.prefix}-veda-vector_ingest"
    }
  ]
}

module "custom_policy" {
  source             = "./custom_policies"
  prefix             = var.prefix
  account_id         = data.aws_caller_identity.current.account_id
  cluster_name       = module.mwaa.cluster_name
  mwaa_arn           = module.mwaa.mwaa_arn
  assume_role_arns   = var.assume_role_arns
  region             = local.aws_region
  cognito_app_secret = var.workflows_client_secret
  vector_secret_name = var.vector_secret_name
}

data "aws_subnets" "private" {
  filter {
    name   = "vpc-id"
    values = [var.vector_vpc == null ? "" : var.vector_vpc]
  }

  tags = {
    "Scope" = "private"
  }
}

resource "aws_security_group" "vector_sg" {
  count  = var.vector_vpc == "null" ? 0 : 1
  name   = "${var.prefix}_veda_vector_sg"
  vpc_id = var.vector_vpc

  egress {
    from_port        = 0
    to_port          = 0
    protocol         = "-1"
    cidr_blocks      = ["0.0.0.0/0"]
    ipv6_cidr_blocks = ["::/0"]
  }
}

resource "aws_vpc_security_group_ingress_rule" "vector_rds_ingress" {
  count             = var.vector_vpc == "null" ? 0 : 1
  security_group_id = var.vector_security_group

  from_port                    = 5432
  to_port                      = 5432
  ip_protocol                  = "tcp"
  referenced_security_group_id = aws_security_group.vector_sg[count.index].id
}

resource "local_file" "mwaa_variables" {
  content = templatefile("${path.module}/mwaa_environment_variables.tpl",
    {
      prefix                  = var.prefix
      event_bucket            = module.mwaa.mwaa_s3_name
      securitygroup_1         = module.mwaa.mwaa_security_groups[0]
      subnet_1                = module.mwaa.subnets[0]
      subnet_2                = module.mwaa.subnets[1]
      stage                   = var.stage
      ecs_cluster_name        = module.mwaa.cluster_name
      log_group_name          = module.mwaa.log_group_name
      mwaa_execution_role_arn = module.mwaa.mwaa_role_arn
      account_id              = local.account_id
      aws_region              = local.aws_region
      cognito_app_secret      = var.workflows_client_secret
      stac_ingestor_api_url   = var.stac_ingestor_api_url
      vector_secret_name      = var.vector_secret_name
  })
  filename = "/tmp/mwaa_vars.json"
}

##########################################################
# Workflows API
##########################################################

# ECR repository to host workflows API image
resource "aws_ecr_repository" "workflows_api_lambda_repository" {
  name = "${var.prefix}_workflows-api-lambda-repository"
}

resource "null_resource" "if_change_run_provisioner" {
  triggers = {
    always_run = "${timestamp()}"
  }
  provisioner "local-exec" {
    command = <<-EOT
      set -e
      aws ecr get-login-password --region ${local.aws_region} | docker login --username AWS --password-stdin ${aws_ecr_repository.workflows_api_lambda_repository.repository_url}
      docker build --platform=linux/amd64 -t ${aws_ecr_repository.workflows_api_lambda_repository.repository_url}:latest ../workflows_api/runtime/
      docker push ${aws_ecr_repository.workflows_api_lambda_repository.repository_url}:latest
    EOT
  }
}

# IAM Role for Lambda Execution
resource "aws_iam_role" "lambda_execution_role" {
  name                 = "${var.prefix}_lambda_execution_role"
  permissions_boundary = var.iam_policy_permissions_boundary_name == "null" ? null : "arn:aws:iam::${local.account_id}:policy/${var.iam_policy_permissions_boundary_name}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Action = "sts:AssumeRole",
        Effect = "Allow",
        Principal = {
          Service = "lambda.amazonaws.com",
        },
      },
    ],
  })
}

resource "aws_iam_policy" "lambda_access" {
  name        = "${var.prefix}_Access_For_Lambda"
  path        = "/"
  description = "Access policy for Lambda function"
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
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
        Effect: "Allow",
        Action: "sts:AssumeRole",
        Resource: var.data_access_role_arn
      },
      {
        Action: "airflow:CreateCliToken",
        Resource: [
          "arn:aws:airflow:${var.aws_region}:${local.account_id}:environment/${var.prefix}-mwaa"
        ],
        Effect: "Allow"
      }
    ],
  })
}

resource "aws_iam_role_policy_attachment" "lambda_access_attach" {
  role       = aws_iam_role.lambda_execution_role.name
  policy_arn = aws_iam_policy.lambda_access.arn
}

resource "aws_iam_role_policy_attachment" "lambda_basic_execution" {
  role       = aws_iam_role.lambda_execution_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

# Function to build the JWKS URL
locals {
  build_jwks_url = "${format("https://cognito-idp.%s.amazonaws.com/%s/.well-known/jwks.json", local.aws_region, var.userpool_id)}"
}

data "aws_subnets" "lambda_private" {
  filter {
    name   = "vpc-id"
    values = [var.vpc_id]
  }
  tags = {
    "Scope" = "private"
  }
}

resource "aws_security_group" "lambda_sg" {
  name   = "${var.prefix}_lambda_workflows_sg"
  vpc_id = var.vpc_id

  egress {
    from_port        = 0
    to_port          = 0
    protocol         = "-1"
    cidr_blocks      = ["0.0.0.0/0"]
    ipv6_cidr_blocks = ["::/0"]
  }
}

resource "aws_lambda_function" "workflows_api_handler" {
  function_name = "${var.prefix}_workflows_api_handler"
  role          = aws_iam_role.lambda_execution_role.arn
  package_type  = "Image"
  timeout       = 30
  image_uri = "${aws_ecr_repository.workflows_api_lambda_repository.repository_url}:latest"
  environment {
    variables = {
      WORKFLOWS_CLIENT_SECRET_ID = var.cognito_app_secret
      STAGE                      = var.stage
      DATA_ACCESS_ROLE_ARN       = var.data_access_role_arn
      WORKFLOW_ROOT_PATH         = var.workflow_root_path
      INGEST_URL                 = var.stac_ingestor_api_url
      RASTER_URL                 = var.raster_url
      STAC_URL                   = var.stac_url
      MWAA_ENV                   = "${var.prefix}-mwaa"
      COGNITO_DOMAIN             = var.cognito_domain
      CLIENT_ID                  = var.client_id
      JWKS_URL                   = local.build_jwks_url
    }
  }
  vpc_config {
    subnet_ids = data.aws_subnets.lambda_private.ids
    security_group_ids = [aws_security_group.lambda_sg.id]
  }
}

resource "null_resource" "update_workflows_lambda_image" {
  triggers = {
    always_run = "${timestamp()}"
  }

  depends_on = [aws_lambda_function.workflows_api_handler, null_resource.if_change_run_provisioner]
  provisioner "local-exec" {
    command = <<-EOT
      set -e
      aws lambda update-function-code \
           --function-name ${aws_lambda_function.workflows_api_handler.function_name} \
           --image-uri ${aws_ecr_repository.workflows_api_lambda_repository.repository_url}:latest
    EOT
  }
}

# API Gateway HTTP API
resource "aws_apigatewayv2_api" "workflows_http_api" {
  name          = "${var.prefix}_workflows_http_api"
  protocol_type = "HTTP"
}

# Lambda Integration for API Gateway
resource "aws_apigatewayv2_integration" "workflows_lambda_integration" {
  api_id                 = aws_apigatewayv2_api.workflows_http_api.id
  integration_type       = "AWS_PROXY"
  integration_uri        = aws_lambda_function.workflows_api_handler.invoke_arn
  payload_format_version = "2.0"
}

# Default Route for API Gateway
resource "aws_apigatewayv2_route" "workflows_default_route" {
  api_id    = aws_apigatewayv2_api.workflows_http_api.id
  route_key = "$default"
  target    = "integrations/${aws_apigatewayv2_integration.workflows_lambda_integration.id}"
}

resource "aws_apigatewayv2_stage" "workflows_default_stage" {
  api_id      = aws_apigatewayv2_api.workflows_http_api.id
  name        = "$default"
  auto_deploy = true
}

resource "aws_lambda_permission" "api-gateway" {
  statement_id  = "AllowExecutionFromAPIGateway"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.workflows_api_handler.arn
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_apigatewayv2_api.workflows_http_api.execution_arn}/*/$default"
}

# Cloudfront update

resource "null_resource" "update_cloudfront" {
  triggers = {
    always_run = "${timestamp()}"
  }

  count = coalesce(var.cloudfront_id, false) ? 1 : 0

  provisioner "local-exec" {
    command = "${path.module}/cf_update.sh ${var.cloudfront_id} workflows_api_origin \"${aws_apigatewayv2_api.workflows_http_api.api_endpoint}\""
  }

  depends_on = [aws_apigatewayv2_api.workflows_http_api]
}