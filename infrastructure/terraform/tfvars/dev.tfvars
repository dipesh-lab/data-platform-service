env_name = "dev"
aws_region = "ap-southeast-2"
deployer_role_arn = "arn:aws:iam::128779316957:role/app-tf-deployer-dev"

glue_catalog_config = {
  assumeRoleArn   = "arn:aws:iam::128779316957:role/dp-glue-catalog-role-dev"
  catalogDataPath = "/home/tom/projects/javaworkspace/data-platform-service/catalog-data"
}