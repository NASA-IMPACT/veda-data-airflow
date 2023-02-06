module "mwaa" {
  source                           = "https://github.com/amarouane-ABDELHAK/mwaa_tf_module/releases/download/v1.2.4/mwaa_tf_module.zip"
  prefix                           = var.prefix
  vpc_id                           = var.vpc_id
  iam_role_additional_arn_policies = merge(module.custom_policy.custom_policy_arns_map)
  permissions_boundary_arn         = var.iam_role_permissions_boundary
  subnet_tagname                   = var.subnet_tagname
  local_requirement_file_path      = "${path.module}/../dags/requirements.txt"
  local_dag_folder                 = "${path.module}/../dags/"
  mwaa_variables_json_file_id_path = {file_path = local_file.mwaa_variables.filename, file_id =local_file.mwaa_variables.id }

}


resource "aws_ecr_repository" "veda_stac_build" {
  name = "${var.prefix}_veda_stac_build"
  image_scanning_configuration {
    scan_on_push = false
  }
  tags = {
    "name" = "VEDA STAC Build"
  }
}

resource "null_resource" "veda_stac_build_image" {
  triggers = {
    python_file_handler = md5(file("${path.module}/../docker_tasks/build_stac/handler.py"))
    docker_file         = md5(file("${path.module}/../docker_tasks/build_stac/Dockerfile"))
  }

  provisioner "local-exec" {
    command = <<EOF
          cd ${path.module}/../docker_tasks/build_stac
          aws ecr get-login-password --region ${local.aws_region} | docker login --username AWS --password-stdin ${local.account_id}.dkr.ecr.${local.aws_region}.amazonaws.com
          docker build -t ${aws_ecr_repository.veda_stac_build.repository_url}:latest .
          docker push ${aws_ecr_repository.veda_stac_build.repository_url}:latest
       EOF
  }
}
module "veda_ecs_cluster" {
  source                  = "./ecs"
  prefix                  = var.prefix
  aws_region              = local.aws_region
  docker_image_url        = "${aws_ecr_repository.veda_stac_build.repository_url}:latest"
  mwaa_execution_role_arn = module.mwaa.mwaa_role_arn
  mwaa_task_role_arn      = module.mwaa.mwaa_role_arn
  stage                   = var.stage
}


module "custom_policy" {
  source              = "./custom_policies"
  prefix              = var.prefix
  account_id          = data.aws_caller_identity.current.account_id
  aws_log_group_name  = module.veda_ecs_cluster.log_group_name
  aws_log_stream_name = module.veda_ecs_cluster.stream_log_name
  cluster_name        = module.veda_ecs_cluster.cluster_name
  assume_role_arns    = var.assume_role_arns
  region              = local.aws_region
}

resource "local_file" mwaa_variables {
  content  = templatefile("${path.module}/mwaa_environment_variables.tpl",
    {
      prefix = var.prefix
      assume_role_arn = var.assume_role_arns[0] # Just happen to be the one we need is read assume role arn
      event_bucket = module.mwaa.mwaa_s3_name
      securitygroup_1 = module.mwaa.mwaa_security_groups[0]
      subnet_1 = module.mwaa.subnets[0]
      subnet_2 = module.mwaa.subnets[1]
      cognito_app_secret = var.cognito_app_secret
      stac_ingestor_api_url = var.stac_ingestor_api_url
      stage = var.stage

    })
  filename = "/tmp/mwaa_vars.json"
}