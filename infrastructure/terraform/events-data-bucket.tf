locals {
  catalog_data_path = "${path.module}/../../catalog-data"
  table_names       = [
    for file in fileset(local.catalog_data_path, "**/*.json") :
    "${split("/", file)[0]}-${jsondecode(file("${local.catalog_data_path}/${file}")).bucketName}"
  ]
  bucket_names      = toset(local.table_names)
}

resource "aws_s3_bucket" "events_data_bucket" {
  for_each      = local.bucket_names

  bucket        = each.value
  force_destroy = false
  tags          = local.common_tags
}

resource "aws_s3_bucket_policy" "events_data_bucket" {
  for_each = aws_s3_bucket.events_data_bucket

  bucket   = each.value.id
  policy   = jsonencode({
    Version   = "2012-10-17"
    Statement = [
      {
        Sid    = "AllowGlueCatalogRole"
        Effect = "Allow"
        Principal = {
          AWS     = aws_iam_role.dp_glue_catalog.arn
        }
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:ListBucket"
        ]
        Resource = [
          each.value.arn, "${each.value.arn}/*"
        ]
      }
    ]
  })
}
