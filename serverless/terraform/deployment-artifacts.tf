data "aws_s3_bucket" "deployable_artifacts" {
  bucket = "app-artifacts-${var.env_name}-${data.aws_caller_identity.current.account_id}-${var.aws_region}-an"
}

resource "aws_s3_object" "data_platform_serverless_jar" {
  bucket      = data.aws_s3_bucket.deployable_artifacts.id
  key         = "data-platform-serverless"
  source      = var.artifacts["data-platform-serverless"]
  source_hash = filesha256(var.artifacts["data-platform-serverless"])
  tags        = local.common_tags
}

resource "aws_s3_object" "data_platform_serverless_libs" {
  bucket      = data.aws_s3_bucket.deployable_artifacts.id
  key         = "data-platform-serverless-libs"
  source      = var.artifacts["data-platform-serverless-libs"]
  source_hash = filesha256(var.artifacts["data-platform-serverless-libs"])
  tags        = local.common_tags
}