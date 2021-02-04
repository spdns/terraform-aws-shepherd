
locals {
  script_create_csv = "scripts/create_csv.py"
}

resource "aws_s3_bucket_object" "create_csv" {
  bucket = module.glue_tmp_bucket.id
  key    = local.script_create_csv
  source = "${path.module}/${local.script_create_csv}"
  etag   = filemd5("${path.module}/${local.script_create_csv}")
}

resource "aws_glue_job" "create_csv" {
  count = length(var.subscriber_buckets)

  name        = format("%s-%s-%s-create-csv", var.project, var.environment, var.subscriber_buckets[count.index])
  description = format("The %s %s job to create the CSV from shepherd data for %s", var.project, var.environment, var.subscriber_buckets[count.index])
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
    // Athena
    "--athenaDatabase" = split(":", aws_glue_catalog_database.shepherd[count.index].id)[1]
    "--athenaTable"    = local.table_name
    // Results
    "--outputBucket" = aws_s3_bucket.csv_results.id
    "--outputDir"    = var.subscriber_buckets[count.index]
    // Salt
    "--salt" = data.aws_ssm_parameter.salt.value
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

resource "aws_glue_trigger" "start_workflow_create_csv" {
  count = length(var.subscriber_buckets)

  name     = format("%s %s schedule triggers etl job to create CSV for %s", var.project, var.environment, var.subscriber_buckets[count.index])
  type     = "SCHEDULED"
  schedule = "cron(1 0/1 * * ? *)"

  actions {
    job_name = aws_glue_job.create_csv[count.index].name
  }

  tags = local.project_tags
}
