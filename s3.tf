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

locals {
  csv_bucket_name = format("%s-%s-%s-%s.%s",
    data.aws_iam_account_alias.current.account_alias,
    data.aws_region.current.name,
    var.project,
    var.environment,
    var.domain,
  )
}

data "aws_iam_policy_document" "supplemental_policy" {

  source_json = var.csv_custom_bucket_policy

  #
  # Enforce SSL/TLS on all transmitted objects
  # We do this by extending the custom_bucket_policy
  #
  statement {
    sid    = "enforce-tls-requests-only"
    effect = "Deny"
    principals {
      type        = "AWS"
      identifiers = ["*"]
    }
    actions = ["s3:*"]
    resources = [
      "arn:${data.aws_partition.current.partition}:s3:::${local.csv_bucket_name}/*"
    ]
    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }
}

resource "aws_s3_bucket" "csv_results" {

  bucket = local.csv_bucket_name
  acl    = "public-read"
  policy = data.aws_iam_policy_document.supplemental_policy.json
  tags   = local.project_tags

  versioning {
    enabled = true
  }

  lifecycle_rule {
    enabled = true

    abort_incomplete_multipart_upload_days = 14

    expiration {
      expired_object_delete_marker = true
    }

    noncurrent_version_expiration {
      days = 90
    }
  }

  server_side_encryption_configuration {
    rule {
      apply_server_side_encryption_by_default {
        sse_algorithm = "AES256"
      }
    }
  }

  logging {
    target_bucket = module.aws_logs.aws_logs_bucket
    target_prefix = format("s3/%s/", local.csv_bucket_name)
  }
}

module "aws_logs" {
  source  = "trussworks/logs/aws"
  version = "~> 10.0.0"

  s3_bucket_name = format("%s-%s-%s-aws-logs", data.aws_region.current.name, var.project, var.environment)
  tags           = local.project_tags
}
