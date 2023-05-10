module "mwaa" {
  source                           = "https://github.com/NASA-IMPACT/mwaa_tf_module/releases/download/v1.1.2/mwaa_tf_module.zip"
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
  cognito_app_secret = var.cognito_app_secret
  vector_secret_name = var.vector_secret_name
}


data "aws_subnets" "private" {
  filter {
    name   = "vpc-id"
    values = [var.vector_vpc]
  }

  tags = {
    "Scope" = "private"
  }
}

resource "aws_security_group" "vector_sg" {
  count = var.vector_vpc == null ? 0: 1
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
  count = var.vector_vpc == null ? 0: 1
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
      cognito_app_secret      = var.cognito_app_secret
      stac_ingestor_api_url   = var.stac_ingestor_api_url

  })
  filename = "/tmp/mwaa_vars.json"
}


#module "mwaa" {
#  source                           = "https://github.com/amarouane-ABDELHAK/mwaa_tf_module/releases/download/v1.2.4/mwaa_tf_module.zip"
#  prefix                           = var.prefix
#  vpc_id                           = var.vpc_id
#  iam_role_additional_arn_policies = merge(module.custom_policy.custom_policy_arns_map)
#  permissions_boundary_arn         = var.iam_role_permissions_boundary
#  subnet_tagname                   = var.subnet_tagname
#  local_requirement_file_path      = "${path.module}/../dags/requirements.txt"
#  local_dag_folder                 = "${path.module}/../dags/"
#  mwaa_variables_json_file_id_path = {file_path = local_file.mwaa_variables.filename, file_id =local_file.mwaa_variables.id }
#
#}
#
#
#resource "aws_ecr_repository" "veda_stac_build" {
#  name = "${var.prefix}_veda_stac_build"
#  image_scanning_configuration {
#    scan_on_push = false
#  }
#  tags = {
#    "name" = "VEDA STAC Build"
#  }
#}
#
#resource "null_resource" "veda_stac_build_image" {
#  triggers = {
#    python_file_handler = md5(file("${path.module}/../docker_tasks/build_stac/handler.py"))
#    docker_file         = md5(file("${path.module}/../docker_tasks/build_stac/Dockerfile"))
#  }
#
#  provisioner "local-exec" {
#    command = <<EOF
#          cd ${path.module}/../docker_tasks/build_stac
#          aws ecr get-login-password --region ${local.aws_region} | docker login --username AWS --password-stdin ${local.account_id}.dkr.ecr.${local.aws_region}.amazonaws.com
#          docker build -t ${aws_ecr_repository.veda_stac_build.repository_url}:latest .
#          docker push ${aws_ecr_repository.veda_stac_build.repository_url}:latest
#       EOF
#  }
#}
#module "veda_ecs_cluster" {
#  source                  = "./ecs"
#  prefix                  = var.prefix
#  aws_region              = local.aws_region
#  docker_image_url        = "${aws_ecr_repository.veda_stac_build.repository_url}:latest"
#  mwaa_execution_role_arn = module.mwaa.mwaa_role_arn
#  mwaa_task_role_arn      = module.mwaa.mwaa_role_arn
#  stage                   = var.stage
#}
#
#
#module "custom_policy" {
#  source              = "./custom_policies"
#  prefix              = var.prefix
#  account_id          = data.aws_caller_identity.current.account_id
#  aws_log_group_name  = module.veda_ecs_cluster.log_group_name
#  aws_log_stream_name = module.veda_ecs_cluster.stream_log_name
#  cluster_name        = module.veda_ecs_cluster.cluster_name
#  assume_role_arns    = var.assume_role_arns
#  region              = local.aws_region
#}
#
#resource "local_file" mwaa_variables {
#  content  = templatefile("${path.module}/mwaa_environment_variables.tpl",
#    {
#      prefix = var.prefix
#      assume_role_arn = var.assume_role_arns[0] # Just happen to be the one we need is read assume role arn
#      event_bucket = module.mwaa.mwaa_s3_name
#      securitygroup_1 = module.mwaa.mwaa_security_groups[0]
#      subnet_1 = module.mwaa.subnets[0]
#      subnet_2 = module.mwaa.subnets[1]
#      cognito_app_secret = var.cognito_app_secret
#      stac_ingestor_api_url = var.stac_ingestor_api_url
#      stage = var.stage
#
#    })
#  filename = "/tmp/mwaa_vars.json"
#}