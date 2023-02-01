terraform {
  required_providers {
    aws = {
      version = "~> 4.0"
    }
  }
}


provider "aws" {
  profile = var.aws_profile
  region  = var.aws_region
}

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

locals {
  aws_region = data.aws_region.current.name
  account_id = data.aws_caller_identity.current.account_id
}


data "aws_subnets" "subnet_ids" {
  filter {
    name   = "tag:Name"
    values = [var.subnet_tagname]
  }
  filter {
    name   = "vpc-id"
    values = [var.vpc_id]
  }

}