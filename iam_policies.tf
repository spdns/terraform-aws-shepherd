#
# Assume Role
#
data "aws_iam_policy_document" "assume_role_policy" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "AWS"
      identifiers = [data.aws_caller_identity.current.account_id]
    }
    condition {
      test     = "Bool"
      variable = "aws:MultiFactorAuthPresent"
      values   = ["true"]
    }
  }
}

#
# Shepherd Users
#
data "aws_iam_policy_document" "shepherd_users" {
  // Allow all actions against athena results bucket
  statement {
    effect = "Allow"
    actions = [
      "s3:*",
    ]
    resources = [
      module.athena_results.arn,
      "${module.athena_results.arn}/*",
    ]
    condition {
      test     = "Bool"
      variable = "aws:MultiFactorAuthPresent"
      values   = ["true"]
    }
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
    condition {
      test     = "Bool"
      variable = "aws:MultiFactorAuthPresent"
      values   = ["true"]
    }
  }

  // Allow full access to athena against the workgroup
  statement {
    effect = "Allow"
    actions = [
      "athena:*",
    ]
    resources = aws_athena_workgroup.shepherd[*].arn
    condition {
      test     = "Bool"
      variable = "aws:MultiFactorAuthPresent"
      values   = ["true"]
    }
  }

  // Allow full access to athena against the workgroup
  statement {
    effect = "Allow"
    actions = [
      "athena:Get*",
      "athena:List*",
    ]
    resources = ["*"]
    condition {
      test     = "Bool"
      variable = "aws:MultiFactorAuthPresent"
      values   = ["true"]
    }
  }

  // Allow decrypt of all AWS resources using AWS managed KMS keys
  statement {
    effect = "Allow"
    actions = [
      "kms:ListAliases",
      "kms:Decrypt",
    ]
    resources = ["*"] // This should apply only to AWS KMS keys where the principal can be `*`.
    condition {
      test     = "Bool"
      variable = "aws:MultiFactorAuthPresent"
      values   = ["true"]
    }
  }

  statement {
    effect = "Allow"
    actions = [
      "glue:*",
    ]
    resources = [
      "*",
    ]
    condition {
      test     = "Bool"
      variable = "aws:MultiFactorAuthPresent"
      values   = ["true"]
    }
  }

  statement {
    effect = "Allow"
    actions = [
      "quicksight:*",
    ]
    resources = [
      "*",
    ]
    condition {
      test     = "Bool"
      variable = "aws:MultiFactorAuthPresent"
      values   = ["true"]
    }
  }

  statement {
    effect = "Allow"
    actions = [
      "tag:*",
    ]
    resources = [
      "*",
    ]
    condition {
      test     = "Bool"
      variable = "aws:MultiFactorAuthPresent"
      values   = ["true"]
    }
  }
}

resource "aws_iam_policy" "shepherd_users" {
  name        = "app-${var.project}-${var.environment}"
  description = "Policy for 'shepherd_users'"
  policy      = data.aws_iam_policy_document.shepherd_users.json
}

resource "aws_iam_role_policy_attachment" "shepherd_users_policy_attachment" {
  role       = aws_iam_role.shepherd_users.name
  policy_arn = aws_iam_policy.shepherd_users.arn
}

#
# Allow group to assume role
#

# Allow assuming the "shepherd_users" role
data "aws_iam_policy_document" "assume_role_shepherd_policy_doc" {
  statement {
    effect    = "Allow"
    actions   = ["sts:AssumeRole"]
    resources = [aws_iam_role.shepherd_users.arn]
  }
}

resource "aws_iam_policy" "assume_role_shepherd_users_policy" {
  name        = "app-${var.project}-${var.environment}-assume-role"
  path        = "/"
  description = "Allows the 'shepherd_users' role to be assumed."
  policy      = data.aws_iam_policy_document.assume_role_shepherd_policy_doc.json
}

resource "aws_iam_group_policy_attachment" "shepherd_users_assume_role_policy_attachment" {
  group      = aws_iam_group.shepherd_users.name
  policy_arn = aws_iam_policy.assume_role_shepherd_users_policy.arn
}
