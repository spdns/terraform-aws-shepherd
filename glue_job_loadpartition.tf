
locals {
  script_loadpartition = "scripts/loadpartition.py"
}

resource "aws_s3_bucket_object" "loadpartition" {
  bucket = module.glue_tmp_bucket.id
  key    = local.script_loadpartition
  source = "${path.module}/${local.script_loadpartition}"
  etag   = filemd5("${path.module}/${local.script_loadpartition}")
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
    "--tableName" = local.table_name
    // Source Data
    "--s3Bucket" = var.subscriber_buckets[count.index]
    "--s3Folder" = "/"
    "--dataType" = "dns"
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

resource "aws_glue_trigger" "start_workflow_loadpartitions" {
  count = length(var.subscriber_buckets)

  name     = format("%s %s schedule triggers etl job to load partitions for %s", var.project, var.environment, var.subscriber_buckets[count.index])
  type     = "SCHEDULED"
  schedule = "cron(1 0/1 * * ? *)"

  actions {
    job_name = aws_glue_job.shepherd[count.index].name
  }

  tags = local.project_tags
}

resource "aws_glue_job" "shepherd_proxy" {
  count = length(var.subscriber_buckets)

  name        = format("%s-%s-%s-proxy", var.project, var.environment, var.subscriber_buckets[count.index])
  description = format("The %s %s job to update shepherd data for %s for proxy data", var.project, var.environment, var.subscriber_buckets[count.index])
  role_arn    = aws_iam_role.glue_role.arn

  glue_version = "2.0"

  command {         
    name            = "pythonshell"
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
    "--tableName" = local.proxy_table_name
    // Source Data
    "--s3Bucket" = var.subscriber_buckets[count.index]
    "--s3Folder" = "/"
    "--dataType" = "proxy"
  }

  execution_property {
    max_concurrent_runs = 1
  }

  security_configuration = aws_glue_security_configuration.event_data.id

  timeout       = 10     // minutes
  max_capacity = 0.0625  // Update to 1.0 if needed.

  tags = local.project_tags
}

resource "aws_glue_trigger" "start_workflow_loadpartitions_proxy" {
  count = length(var.subscriber_buckets)

  name     = format("%s %s schedule triggers etl job to load partitions for %s to proxy table", var.project, var.environment, var.subscriber_buckets[count.index])
  type     = "SCHEDULED"
  schedule = "cron(2 0/1 * * ? *)"

  actions {
    job_name = aws_glue_job.shepherd_proxy[count.index].name
  }

  tags = local.project_tags
}
