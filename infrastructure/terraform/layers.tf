resource "null_resource" "catalog_data_layer" {
  triggers = {
    always_run = timestamp()
  }

  provisioner "local-exec" {
    command = <<-EOT
      tar -czf catalog.tar.gz --transform 's^catalog-data^java/catalog-data^' ../../catalog-data
      tar -xzf catalog.tar.gz && zip -r catalog.zip java && rm -rf java
      rm -rf catalog.tag.gz
    EOT
  }
}

resource "aws_lambda_layer_version" "catalog_data_layer" {
  layer_name          = "catalog-data-${var.env_name}"
  compatible_runtimes = ["java21"]
  filename            = "${path.module}/catalog.zip"
  source_code_hash    = filebase64sha256("${path.module}/catalog.zip")
}