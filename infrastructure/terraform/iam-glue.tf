resource "aws_iam_role" "dp_glue_catalog" {
  name = "dp-glue-catalog-role-${var.env_name}"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Sid    = ""
        Principal = {
          AWS = ["arn:aws:iam::${data.aws_caller_identity.current.account_id}:root", var.deployer_role_arn]
        }
      },
    ]
  })
}

resource "aws_iam_role_policy" "dp_glue_catalog" {
  role = aws_iam_role.dp_glue_catalog.id
  name = "dp-glue-catalog-rp-${var.env_name}"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = [
          "glue:GetDatabase",
          "glue:CreateDatabase",
          "glue:GetTable",
          "glue:CreateTable",
          "glue:UpdateTable",
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ]
        Effect   = "Allow"
        Resource = "*"
      }
    ]
  })
}
