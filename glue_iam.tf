
data "aws_iam_policy_document" "glue_assume_role_policy_document" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
    effect = "Allow"
  }
}

resource "aws_iam_role" "glue_role" {
  name               = format("%s-%s-glue-role", var.project, var.environment)
  description        = format("%s-%s: Assume role policy for AWS Glue", var.project, var.environment)
  assume_role_policy = data.aws_iam_policy_document.glue_assume_role_policy_document.json

  tags = local.project_tags
}

resource "aws_iam_role_policy_attachment" "glue_role_att" {
  role       = aws_iam_role.glue_role.name
  policy_arn = format("arn:%s:iam::aws:policy/service-role/AWSGlueServiceRole", data.aws_partition.current.partition)
}

data "aws_iam_policy_document" "glue_policy_document" {

  statement {
    sid = "ReadOnlyFromBuckets"
    actions = [
      "s3:Get*",
      "s3:List*",
    ]
    effect = "Allow"
    resources = flatten([for bucket in var.subscriber_buckets : [
      format("arn:%s:s3:::%s", data.aws_partition.current.partition, bucket),
      format("arn:%s:s3:::%s/*", data.aws_partition.current.partition, bucket),
      ]
    ])
  }

  statement {
    sid = "AthenaGlueAccess"
    actions = [
      "s3:GetBucketLocation",
      "s3:GetObject",
      "s3:ListBucket",
      "s3:ListBucketMultipartUploads",
      "s3:ListMultipartUploadParts",
      "s3:ListAllMyBuckets",
      "s3:AbortMultipartUpload",
      "s3:CreateBucket",
      "s3:PutObject",
      "s3:DeleteObject",
    ]
    effect = "Allow"
    resources = [
      module.athena_results.arn,
      "${module.athena_results.arn}/*",
    ]
  }

  statement {
    sid = "GlueCSVResults"
    actions = [
      "s3:*",
    ]
    effect = "Allow"
    resources = [
      module.glue_tmp_bucket.arn,
      "${module.glue_tmp_bucket.arn}/*",
      aws_s3_bucket.csv_results.arn,
      "${aws_s3_bucket.csv_results.arn}/*",
    ]
  }

  statement {
    sid = "DecryptS3Files"
    actions = [
      "kms:Decrypt",
    ]
    effect    = "Allow"
    resources = ["*"]
  }

  statement {
    actions = [
      "athena:*",
    ]
    effect    = "Allow"
    resources = ["*"]
  }

  statement {
    actions = [
      "glue:*",
    ]
    effect = "Allow"
    resources = flatten([
      [format("arn:%s:glue:%s:%s:catalog",
        data.aws_partition.current.partition,
        data.aws_region.current.name,
      data.aws_caller_identity.current.account_id)],
      [for db in aws_glue_catalog_database.shepherd : [
        db.arn,
        format("%s/%s", db.arn, local.table_name),
        ]
    ]])
  }

  statement {
    actions = [
      "sns:ListTopics",
      "sns:GetTopicAttributes",
    ]
    effect    = "Allow"
    resources = ["*"]
  }

  statement {
    actions = [
      "cloudwatch:CreateLogGroup",
      "cloudwatch:PutMetricAlarm",
      "cloudwatch:DescribeAlarms",
      "cloudwatch:DeleteAlarms",
    ]
    effect    = "Allow"
    resources = ["*"]
  }
}

resource "aws_iam_policy" "glue_policy" {
  name        = format("%s-%s-glue-policy", var.project, var.environment)
  description = format("%s-%s: Allow AWS Glue to read from resources", var.project, var.environment)
  policy      = data.aws_iam_policy_document.glue_policy_document.json
}

resource "aws_iam_role_policy_attachment" "glue_policy_att" {
  role       = aws_iam_role.glue_role.name
  policy_arn = aws_iam_policy.glue_policy.arn
}
