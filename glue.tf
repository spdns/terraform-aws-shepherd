
locals {
  glue_database_name_prefix = format("%s-%s-database", var.project, var.environment)
}

resource "aws_glue_catalog_database" "shepherd" {
  count = length(var.subscriber_buckets)

  // https://aws.amazon.com/premiumsupport/knowledge-center/parse-exception-missing-eof-athena/
  name        = replace(replace(format("%s-%s", local.glue_database_name_prefix, var.subscriber_buckets[count.index]), "-", "_"), ".", "_")
  description = format("The %s %s database holding data", var.project, var.environment)
  catalog_id  = data.aws_caller_identity.current.account_id
}

resource "aws_glue_security_configuration" "event_data" {
  name = format("%s-event-data-security-config", var.project)

  encryption_configuration {
    cloudwatch_encryption {
      cloudwatch_encryption_mode = "DISABLED"
    }

    job_bookmarks_encryption {
      job_bookmarks_encryption_mode = "DISABLED"
    }

    s3_encryption {
      s3_encryption_mode = "SSE-S3"
    }
  }
}
