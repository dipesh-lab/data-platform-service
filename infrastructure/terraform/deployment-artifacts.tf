/*data "aws_s3_bucket" "deployable_artifacts" {
  bucket = "app-deployable-artifacts-${var.env_name}"
}

resource "aws_s3_object" "data_platform_infra_jar" {
  bucket      = data.aws_s3_bucket.deployable_artifacts.id
  key         = "data-platform-service-infra"
  source      = var.artifacts["data-platform-service-infra"]
  source_hash = filesha256(var.artifacts["data-platform-service-infra"])
  tags        = local.common_tags
}*/