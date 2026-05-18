module "apply_catalog_schema_lambda" {
  source  = "terraform-aws-modules/lambda/aws"
  version = "8.8.0"

  function_name       = "apply-catalog-schema-${var.env_name}"
  description         = "Apply Catalog Schema lambda"
  handler             = "com.dataplatform.catalog.handlers.CatalogSchemaHandler::handleRequest"
  create_package      = false
  publish             = true
  s3_existing_package = {
    bucket     = data.aws_s3_bucket.deployable_artifacts.id
    key        = aws_s3_object.data_platform_infra_jar.key
    version_id = aws_s3_object.data_platform_infra_jar.version_id
  }

  environment_variables = {
    CATALOG_DATA_PATH = var.glue_catalog_config.catalogDataPath
    GLUE_ASSUME_ROLE_ARN = var.glue_catalog_config.assumeRoleArn
  }

  attach_policy_statements = true
  policy_statements        = {
    stsRole = {
      effect = "Allow",
      actions = ["sts:AssumeRole"],
      resources = [aws_iam_role.dp_glue_catalog.arn]
    }
  }

  layers = [aws_lambda_layer_version.catalog_data_layer.arn]
  cloudwatch_logs_retention_in_days = local.default_log_retention_days
  runtime                           = local.java_runtime
  memory_size                       = local.default_lambda_max_memory
  timeout                           = local.default_lambda_timeout
  include_default_tag               = false
  tags                              = local.common_tags

  depends_on = [
    aws_lambda_layer_version.catalog_data_layer
  ]
}