variable "env_name" {
  description = "dev/qa/stg/prod"
  type        = string
  default     = "dev"
}

variable "aws_region" {
  description = "Target AWS region"
  type        = string
}

variable "deployer_role_arn" {
  description = "Assume role arn for deployment"
  type        = string
}

locals {
  common_tags = {
    project = "DataPlatform"
    env     = var.env_name
  }
}