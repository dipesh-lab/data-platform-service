resource "aws_s3_bucket" "athena_query_results" {
  bucket        = "dp-query-results-${var.env_name}"
  force_destroy = false
  tags          = local.common_tags
}
resource "aws_s3_bucket_server_side_encryption_configuration" "athena_query_results" {
  bucket = aws_s3_bucket.athena_query_results.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}
resource "aws_s3_bucket_lifecycle_configuration" "athena_query_results" {
  bucket = aws_s3_bucket.athena_query_results.id
  rule {
    id     = "cleanup"
    status = "Enabled"
    expiration {
      days = 2
    }
  }
}
resource "aws_s3_bucket_public_access_block" "athena_public_access" {
  bucket = aws_s3_bucket.athena_query_results.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_athena_workgroup" "athena_workgroup" {
  name = "dp-query-results-work-${var.env_name}"
  force_destroy = false

  configuration {
    enforce_workgroup_configuration = true
    publish_cloudwatch_metrics_enabled = true
    engine_version {
      selected_engine_version = "Athena engine version 3"
    }
    result_configuration {
      output_location = "s3://${aws_s3_bucket.athena_query_results.bucket}/results/"
      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
  tags = local.common_tags

  depends_on = [
    aws_s3_bucket.athena_query_results
  ]
}