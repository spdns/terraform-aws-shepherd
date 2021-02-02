module "athena_results" {
  source  = "trussworks/s3-private-bucket/aws"
  version = "~> 3.2.1"

  bucket = format("%s-%s-%s-athena-results", data.aws_region.current.name, var.project, var.environment)
  tags   = local.project_tags

  logging_bucket = module.aws_logs.aws_logs_bucket
}

module "glue_tmp_bucket" {
  source  = "trussworks/s3-private-bucket/aws"
  version = "~> 3.2.1"

  bucket = format("%s-%s-%s-glue-tmp", data.aws_region.current.name, var.project, var.environment)
  tags   = local.project_tags

  logging_bucket = module.aws_logs.aws_logs_bucket
}

module "aws_logs" {
  source  = "trussworks/logs/aws"
  version = "~> 10.0.0"

  s3_bucket_name = format("%s-%s-%s-aws-logs", data.aws_region.current.name, var.project, var.environment)
  tags           = local.project_tags
}
