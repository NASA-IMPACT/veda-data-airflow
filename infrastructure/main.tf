module "mwaa" {
  source                           = "https://github.com/NASA-IMPACT/mwaa_tf_module/releases/download/v1.1.7.0/mwaa_tf_module.zip"
  prefix                           = var.prefix
  vpc_id                           = var.vpc_id
  iam_role_additional_arn_policies = merge(module.custom_policy.custom_policy_arns_map)
  permissions_boundary_arn         = var.iam_role_permissions_boundary
  subnet_tagname                   = var.subnet_tagname
  local_requirement_file_path      = "${path.module}/../dags/requirements.txt"
  local_dag_folder                 = "${path.module}/../dags/"
  mwaa_variables_json_file_id_path = { file_path = local_file.mwaa_variables.filename, file_id = local_file.mwaa_variables.id }
  stage                            = var.stage
  airflow_version                  = "2.4.3"
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
    # {
    #   handler_file_path         = "${path.module}/../docker_tasks/vector_ingest/handler.py"
    #   docker_file_path          = "${path.module}/../docker_tasks/vector_ingest/Dockerfile"
    #   ecs_container_folder_path = "${path.module}/../docker_tasks/vector_ingest"
    #   ecr_repo_name             = "${var.prefix}-veda-vector_ingest"
    # }
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
  cognito_app_secret = var.cognito_app_secret
  # vector_secret_name = var.vector_secret_name
}


# data "aws_subnets" "private" {
#   filter {
#     name   = "vpc-id"
#     values = [var.vector_vpc]
#   }

#   tags = {
#     "Scope" = "private"
#   }
# }

# resource "aws_security_group" "vector_sg" {
#   name   = "${var.prefix}_veda_vector_sg"
#   vpc_id = var.vector_vpc

#   egress {
#     from_port        = 0
#     to_port          = 0
#     protocol         = "-1"
#     cidr_blocks      = ["0.0.0.0/0"]
#     ipv6_cidr_blocks = ["::/0"]
#   }
# }

# resource "aws_vpc_security_group_ingress_rule" "vector_rds_ingress" {
#   security_group_id = var.vector_security_group

#   from_port                    = 5432
#   to_port                      = 5432
#   ip_protocol                  = "tcp"
#   referenced_security_group_id = aws_security_group.vector_sg.id
# }

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
      cognito_app_secret      = var.cognito_app_secret
      stac_ingestor_api_url   = var.stac_ingestor_api_url
      assume_role_read_arn    = var.assume_role_arns[0]
      assume_role_write_arn   = var.assume_role_arns[1]
      # vector_secret_name      = var.vector_secret_name
      # vector_subnet_1         = data.aws_subnets.private.ids[0]
      # vector_subnet_2         = data.aws_subnets.private.ids[1]
      # vector_security_group   = aws_security_group.vector_sg.id
      # vector_vpc              = var.vector_vpc
  })
  filename = "/tmp/mwaa_vars.json"
}

##########################################################
# Workflows API
##########################################################

# ECR repository to host workflows API image
resource "aws_ecr_repository" "workflows_api_lambda_repository" {
  name = "veda_${var.stage}_workflows-api-lambda-repository"
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
  name = "veda_${var.stage}_lambda_execution_role"

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

resource "aws_iam_policy" "ecr_access" {
  name        = "veda_${var.stage}_ECR_Access_For_Lambda"
  path        = "/"
  description = "ECR access policy for Lambda function"
  policy      = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect   = "Allow",
        Action   = [
          "ecr:GetDownloadUrlForLayer",
          "ecr:BatchGetImage",
        ],
        Resource = "${aws_ecr_repository.workflows_api_lambda_repository.arn}",
      },
    ],
  })
}

resource "aws_iam_role_policy_attachment" "ecr_access_attach" {
  role       = aws_iam_role.lambda_execution_role.name
  policy_arn = aws_iam_policy.ecr_access.arn
}

resource "aws_iam_role_policy_attachment" "lambda_basic_execution" {
  role       = aws_iam_role.lambda_execution_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}


resource "aws_lambda_function" "workflows_api_handler" {
  function_name = "veda_${var.stage}_workflows_api_handler"
  role          = aws_iam_role.lambda_execution_role.arn
  package_type = "Image"
  timeout = 30

  image_uri = "${aws_ecr_repository.workflows_api_lambda_repository.repository_url}:latest"
  environment {
    variables = {
      JWKS_URL          = var.jwks_url
      USERPOOL_ID       = var.cognito_userpool_id
      CLIENT_ID         = var.cognito_client_id
      CLIENT_SECRET     = var.cognito_client_secret
      STAGE             = var.stage
      DATA_ACCESS_ROLE_ARN = var.data_access_role_arn
      WORKFLOW_ROOT_PATH = var.workflow_root_path
      INGEST_URL = var.ingest_url
      RASTER_URL = var.raster_url
      STAC_URL = var.stac_url
      MWAA_ENV = module.mwaa.airflow_url
    }
  }
}


# API Gateway HTTP API
resource "aws_apigatewayv2_api" "workflows_http_api" {
  name          = "veda_${var.stage}_workflows_http_api"
  protocol_type = "HTTP"
}

# Lambda Integration for API Gateway
resource "aws_apigatewayv2_integration" "workflows_lambda_integration" {
  api_id           = aws_apigatewayv2_api.workflows_http_api.id
  integration_type = "AWS_PROXY"
  integration_uri  = aws_lambda_function.workflows_api_handler.invoke_arn
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

  count = var.cloudfront_id ? 1 : 0

  provisioner "local-exec" {
    command = "${path.module}/cf_update.sh ${var.cloudfront_id} workflows_api_origin \"${aws_apigatewayv2_api.workflows_http_api.api_endpoint}\""
  }

  depends_on = [aws_apigatewayv2_api.workflows_http_api]
}