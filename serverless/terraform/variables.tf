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
  description = "Dictionary of artifact name with its file name"
  type        = map(string)
  default     = {
    "data-platform-serverless": "../target/data-platform-serverless-app.jar"
    "data-platform-serverless-libs": "../target/data-platform-serverless-libs.zip"
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
  default_log_retention_days= 30
}