provider "aws" {
  region = var.aws_region
  assume_role {
    role_arn = var.deployer_role_arn
  }
}

terraform {
  required_version = ">= 1.15.3"

  required_providers {
    aws = {
      source = "hashicorp/aws"
      version = "6.45.0"
    }
    null = {
      source  = "hashicorp/null"
      version = "3.2.4"
    }
  }

  backend "s3" {
    bucket               = "tfstate-resources-${data.aws_caller_identity.current.account_id}-${data.aws_region.current.id}-an"
    key                  = "data-platform-service-infra.tfstate"
    workspace_key_prefix = "data-platform-service"
  }
}