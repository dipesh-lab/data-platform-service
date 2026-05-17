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

variable "glue_catalog_config" {
  description = "Glue catalog configuration for schema apply and Iceberg client"
  type = object({
    assumeRoleArn   = string
    catalogDataPath = string
  })
}

variable "artifacts" {
  description = "Dictionary of artifacts name and its package name"
  type        = map(string)
  default     = {
    "data-platform-service-infra": "../target/data-platform-service-infra.jar"
  }
}

locals {
  common_tags = {
    project = "DataPlatform"
    env     = var.env_name
  }
  java_runtime              = "java21"
  default_lambda_max_memory = 384
  default_lambda_timeout    = 40
  default_log_retention_days= 10
}