
resource "aws_s3_bucket_object" "index-html" {
  bucket  = aws_s3_bucket.csv_results.id
  key     = "index.html"
  content = "<!DOCTYPE html><html><body>Hello</body></html>"
}

resource "aws_s3_bucket_object" "not-found-html" {
  bucket  = aws_s3_bucket.csv_results.id
  key     = "404.html"
  content = "<!DOCTYPE html><html><body>Not Found</body></html>"
}

data "aws_iam_policy_document" "csv_results_policy" {
  # Public Access
  statement {
    sid    = "PublicReadGetObject"
    effect = "Allow"
    principals {
      type        = "*"
      identifiers = ["*"]
    }
    actions = [
      "s3:GetObject",
      // "s3:GetObjectVersion",
    ]
    resources = [
      "arn:${data.aws_partition.current.partition}:s3:::${var.csv_bucket_name}/*"
    ]
    # For limiting to a specific IP address:
    # https://docs.aws.amazon.com/AmazonS3/latest/dev/example-bucket-policies.html#example-bucket-policies-use-case-3
  }

  // # Enforce SSL/TLS on all transmitted objects
  // statement {
  //   sid    = "enforce-tls-requests-only"
  //   effect = "Deny"
  //   principals {
  //     type        = "AWS"
  //     identifiers = ["*"]
  //   }
  //   actions = ["s3:*"]
  //   resources = [
  //     "arn:${data.aws_partition.current.partition}:s3:::${var.csv_bucket_name}/*"
  //   ]
  //   condition {
  //     test     = "Bool"
  //     variable = "aws:SecureTransport"
  //     values   = ["false"]
  //   }
  // }
}

resource "aws_s3_bucket" "csv_results" {

  bucket = var.csv_bucket_name
  acl    = "public-read"
  policy = data.aws_iam_policy_document.csv_results_policy.json
  tags   = local.project_tags

  versioning {
    enabled = true
  }

  website {
    index_document = "index.html"
    error_document = "404.html"
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
    target_prefix = format("s3/%s/", var.csv_bucket_name)
  }
}
