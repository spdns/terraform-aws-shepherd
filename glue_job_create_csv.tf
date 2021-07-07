
locals {
  script_create_csv = "scripts/create_csv.py"
}

resource "aws_s3_bucket_object" "create_csv" {
  bucket = module.glue_tmp_bucket.id
  key    = local.script_create_csv
  source = "${path.module}/${local.script_create_csv}"
  etag   = filemd5("${path.module}/${local.script_create_csv}")
}

locals {
  timeout_minutes = 30
}

resource "aws_glue_job" "create_csv" {
  count = length(var.csv_jobs)

  name        = format("%s-%s-%s-create-csv", var.project, var.environment, replace(var.csv_jobs[count.index]["Name"], " ", "-"))
  description = format("The %s %s job to create the CSV from shepherd data for %s", var.project, var.environment, var.csv_jobs[count.index]["Name"])
  role_arn    = aws_iam_role.glue_role.arn

  glue_version = "1.0"

  command {
    name            = "pythonshell"
    python_version  = "3"
    script_location = format("s3://%s/%s", module.glue_tmp_bucket.id, aws_s3_bucket_object.create_csv.key)
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

    /*
    /* Script Inputs below Here
     */
    "--region" = data.aws_region.current.name
    // Athena
    "--athenaDatabase" = replace(replace(format("%s-%s", local.glue_database_name_prefix, var.csv_jobs[count.index]["Bucket"]), "-", "_"), ".", "_")
    // Testing for Proxy Data CSV's
    "--athenaTable" = var.csv_jobs[count.index]["TableName"]
    // Parent Policies
    "--parentPolicies" = var.csv_jobs[count.index]["Policies"]
    // Date Range
    "--maxHoursAgo" = var.csv_jobs[count.index]["HoursAgo"]
    "--fullDays"    = "true"
    // Results
    "--outputBucket"       = aws_s3_bucket.csv_results.id
    "--outputDir"          = "csv"
    "--outputFilename"     = var.csv_jobs[count.index]["OutputFilename"]
    "--salt"               = data.aws_ssm_parameter.salt.value
    "--ordinal"            = var.csv_jobs[count.index]["Ordinal"]
    "--subscriber"         = var.csv_jobs[count.index]["Subscriber"]
    "--receiver"           = var.csv_jobs[count.index]["Receiver"]
    "--verbose"            = "true"
    "--timeout_sec"        = local.timeout_minutes * 60
    "--deleteMetadataFile" = "true"
    "--workgroup"          = format("%s-%s-workgroup-%s", var.project, var.environment, var.csv_jobs[count.index]["Bucket"])
  }

  execution_property {
    max_concurrent_runs = 1
  }

  security_configuration = aws_glue_security_configuration.event_data.id

  timeout      = local.timeout_minutes // minutes
  max_capacity = 0.0625                // Update to 1.0 if needed, but most of the work happens in Athena, not Glue.

  tags = local.project_tags
}

resource "aws_glue_trigger" "start_workflow_create_csv" {
  count = length(var.csv_jobs)

  name     = format("%s %s schedule triggers etl job to create CSV for %s", var.project, var.environment, var.csv_jobs[count.index]["Name"])
  type     = "SCHEDULED"
  schedule = "cron(1 0/1 * * ? *)"

  actions {
    job_name = aws_glue_job.create_csv[count.index].name
  }

  tags = local.project_tags
}
