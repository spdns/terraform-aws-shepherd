
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

locals {
  scripts_loc = "scripts/loadpartition.py"
}

resource "aws_s3_bucket_object" "loadpartition" {
  bucket = module.glue_tmp_bucket.id
  key    = local.scripts_loc
  source = "${path.module}/${local.scripts_loc}"

  # The filemd5() function is available in Terraform 0.11.12 and later
  # For Terraform 0.11.11 and earlier, use the md5() function and the file() function:
  # etag = "${md5(file("path/to/file"))}"
  etag = filemd5("${path.module}/${local.scripts_loc}")
}

resource "aws_glue_job" "shepherd" {
  count = length(var.subscriber_buckets)

  name        = format("%s-%s-%s", var.project, var.environment, var.subscriber_buckets[count.index])
  description = format("The %s %s job to update shepherd data for %s", var.project, var.environment, var.subscriber_buckets[count.index])
  role_arn    = aws_iam_role.glue_role.arn

  glue_version = "2.0"

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = format("s3://%s/%s", module.glue_tmp_bucket.id, aws_s3_bucket_object.loadpartition.key)
  }

  // See https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-glue-arguments.html
  default_arguments = {
    // Buckets prefixed with 'aws-glue' can be created using the AWS Glue IAM role but we are not using it here
    "--TempDir" = format("s3://%s/aws-glue/temp/",
      module.glue_tmp_bucket.id,
    )
    "--job-bookmark-option" = "job-bookmark-disable"
    "--job-language"        = "python"

    // Logging
    "--enable-continuous-cloudwatch-log" = true
    "--enable-continuous-log-filter"     = true

    // Metrics
    "--enable-metrics" = ""

    /*
    /* Script Inputs below Here
     */
    "--region" = data.aws_region.current.name
    // Athena Results
    "--athenaResultFolder" = var.subscriber_buckets[count.index]
    "--athenaResultBucket" = module.athena_results.id
    "--athenaWorkgroup"    = aws_athena_workgroup.shepherd[count.index].id
    // Database Details
    "--database"  = split(":", aws_glue_catalog_database.shepherd[count.index].id)[1]
    "--tableName" = "dns_data"
    // Source Data
    "--s3Bucket" = var.subscriber_buckets[count.index]
    "--s3Folder" = "/"
  }

  execution_property {
    max_concurrent_runs = 1
  }

  security_configuration = aws_glue_security_configuration.event_data.id

  number_of_workers = 10 // Using too many workers can cause write issues with AWS S3
  timeout           = 10 // minutes
  worker_type       = "G.1X"

  tags = local.project_tags
}

resource "aws_glue_trigger" "start_workflow" {
  count = length(var.subscriber_buckets)

  name     = format("%s %s schedule triggers etl job for %s", var.project, var.environment, var.subscriber_buckets[count.index])
  type     = "SCHEDULED"
  schedule = "cron(1 0/1 * * ? *)"

  actions {
    job_name = aws_glue_job.shepherd[count.index].name
  }

  tags = local.project_tags
}
