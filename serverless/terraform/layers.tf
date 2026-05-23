resource "aws_lambda_layer_version" "catalog_data_layer" {
  layer_name          = "catalog-data-${var.env_name}"
  compatible_runtimes = ["java21"]
  filename            = "${path.module}/../target/catalog.zip"
  source_code_hash    = filesha256("${path.module}/../target/catalog.zip")
}

resource "aws_lambda_layer_version" "dependencies_layer" {
  s3_bucket           = data.aws_s3_bucket.deployable_artifacts.id
  s3_key              = aws_s3_object.data_platform_serverless_libs.key
  source_code_hash    = filesha256(var.artifacts["data-platform-serverless-libs"])
  layer_name          = "data-platform-serverless-libs-${var.env_name}"
  compatible_runtimes = [local.java_runtime]
  depends_on          = [
    aws_s3_object.data_platform_serverless_libs
  ]
}