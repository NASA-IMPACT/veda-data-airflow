
module "rds_cluster" {
  source         = "terraform-aws-modules/rds-aurora/aws"
  version        = "7.7.1"
  count          = var.stage == "dev" ? 1: 0 # Only provision RDS in dev env
  name           = "${var.prefix}-aurora-rds"
  engine         = "aurora-postgresql"
  engine_version = var.rds_engine_version
  instances = {
    one = {
      instance_class = var.write_rds_instance_class
    }
    two = {
      instance_class = var.read_rds_instance_class
    }
  }
  iam_role_permissions_boundary = var.iam_role_permissions_boundary

  db_parameter_group_parameters = [
    {
      name = "password_encryption"
      value : "md5"
    }
  ]

  vpc_id                 = var.vpc_id
  subnets                = module.mwaa.subnets
  vpc_security_group_ids = module.mwaa.mwaa_security_groups

  database_name       = var.rds_database_name
  deletion_protection = true
  master_username     = var.rds_username

  apply_immediately   = true
  monitoring_interval = 10

  enabled_cloudwatch_logs_exports = ["postgresql"]

  tags = {
    Environment = var.stage
  }
}
