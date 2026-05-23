data "aws_caller_identity" "current" {}

data "aws_region" "current" {}

data "aws_iam_role" "dp_glue_catalog_role" {
  name = "dp-glue-catalog-role-${var.env_name}"
}