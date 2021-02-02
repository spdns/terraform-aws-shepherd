
data "aws_iam_policy_document" "quicksight_assume_role_policy" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["quicksight.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "quicksight_service" {
  name               = "aws-quicksight-service-role-v0"
  path               = "/service-role/"
  assume_role_policy = data.aws_iam_policy_document.quicksight_assume_role_policy.json
}

resource "aws_iam_role_policy_attachment" "quicksight_athena_policy_att" {
  role       = aws_iam_role.quicksight_service.name
  policy_arn = format("arn:%s:iam::aws:policy/service-role/AWSQuicksightAthenaAccess", data.aws_partition.current.partition)
}

data "aws_iam_policy_document" "quicksight_service" {
  statement {
    effect = "Allow"
    actions = [
      "s3:*",
    ]
    resources = [
      module.athena_results.arn,
      "${module.athena_results.arn}/*",
    ]
  }

  // Allow limited actions against akamai buckets
  statement {
    effect = "Allow"
    actions = [
      "s3:GetBucketLocation",
      "s3:GetBucketRequestPayment",
      "s3:GetEncryptionConfiguration",
      "s3:GetObject",
      "s3:ListBucket",
    ]
    resources = flatten([for bucket in var.subscriber_buckets : [
      format("arn:%s:s3:::%s", data.aws_partition.current.partition, bucket),
      format("arn:%s:s3:::%s/*", data.aws_partition.current.partition, bucket),
      ]
    ])
  }

  // Allow decrypt of all AWS resources using AWS managed KMS keys
  statement {
    effect = "Allow"
    actions = [
      "kms:Decrypt",
    ]
    resources = ["*"]
  }

}

resource "aws_iam_policy" "quicksight_service" {
  name        = format("app-%s-%s-quicksight-service", var.project, var.environment)
  description = "Policy for quicksight-service role"
  policy      = data.aws_iam_policy_document.quicksight_service.json
}

resource "aws_iam_role_policy_attachment" "quicksight_service_policy_attachment" {
  role       = aws_iam_role.quicksight_service.name
  policy_arn = aws_iam_policy.quicksight_service.arn
}
