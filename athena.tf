
# Use workgroups to separate users, teams, applications, or workloads, and to set limits on amount of data each query or the
# entire workgroup can process. You can also view query-related metrics in AWS CloudWatch.
# https://docs.aws.amazon.com/athena/latest/ug/workgroups.html
resource "aws_athena_workgroup" "shepherd" {
  count = length(var.subscriber_buckets)

  name        = format("%s-%s-workgroup-%s", var.project, var.environment, var.subscriber_buckets[count.index])
  description = format("%s %s workgroup for %s", var.project, var.environment, var.subscriber_buckets[count.index])
  state       = "ENABLED"

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    result_configuration {
      output_location = format("s3://%s/%s/", module.athena_results.id, var.subscriber_buckets[count.index])

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }

  tags = local.project_tags
}

locals {
  database_names = [for db in aws_glue_catalog_database.shepherd.*.id : split(":", db)[1]]
  table_name     = "dns_data"
}

resource "aws_athena_named_query" "create_view" {
  count = length(var.subscriber_buckets)

  name      = format("%s-%s-create-view", local.glue_database_name_prefix, var.subscriber_buckets[count.index])
  workgroup = aws_athena_workgroup.shepherd[count.index].id
  database  = split(":", aws_glue_catalog_database.shepherd[count.index].id)[1]
  query     = <<-EOT
CREATE OR REPLACE VIEW shepherd_all
AS
%{for index, db in local.database_names~}
SELECT * FROM ${db}.${local.table_name}
%{if index < length(local.database_names) - 1~}
UNION ALL
%{endif~}
%{endfor~}
GO
EOT
}

data "template_file" "create_table" {
  count = length(var.subscriber_buckets)

  template = file("${path.module}/templates/create_table_spec.sql.tmpl")
  vars = {
    // Query cannot take a database name
    table_name = local.table_name
    s3_bucket  = var.subscriber_buckets[count.index]
  }
}

resource "aws_athena_named_query" "create_table" {
  count = length(var.subscriber_buckets)

  name      = format("%s-%s-create-table", local.glue_database_name_prefix, var.subscriber_buckets[count.index])
  workgroup = aws_athena_workgroup.shepherd[count.index].id
  database  = split(":", aws_glue_catalog_database.shepherd[count.index].id)[1]
  query     = data.template_file.create_table[count.index].rendered
}

data "template_file" "alter_table" {
  count = length(var.subscriber_buckets)

  template = file("${path.module}/templates/alter_table_spec.sql.tmpl")
  vars = {
    // Query cannot take a database name
    table_name = local.table_name
    s3_bucket  = var.subscriber_buckets[count.index]
    subscriber = split("-", var.subscriber_buckets[count.index])[0]
    // These are example values
    year  = "2021"
    month = "1"
    day   = "19"
    hour  = "1611082800"
  }
}

resource "aws_athena_named_query" "alter_table" {
  count = length(var.subscriber_buckets)

  name      = format("%s-%s-alter-table", local.glue_database_name_prefix, var.subscriber_buckets[count.index])
  workgroup = aws_athena_workgroup.shepherd[count.index].id
  database  = split(":", aws_glue_catalog_database.shepherd[count.index].id)[1]
  query     = data.template_file.alter_table[count.index].rendered
}

resource "aws_athena_named_query" "repair_table" {
  count = length(var.subscriber_buckets)

  name      = format("%s-%s-repair-table", local.glue_database_name_prefix, var.subscriber_buckets[count.index])
  workgroup = aws_athena_workgroup.shepherd[count.index].id
  database  = split(":", aws_glue_catalog_database.shepherd[count.index].id)[1]
  // Query cannot take a database name
  query = format("MSCK REPAIR TABLE %s", local.table_name)
}

resource "aws_athena_named_query" "date_range" {
  count = length(var.subscriber_buckets)

  name      = format("%s-%s-date-range", local.glue_database_name_prefix, var.subscriber_buckets[count.index])
  workgroup = aws_athena_workgroup.shepherd[count.index].id
  database  = split(":", aws_glue_catalog_database.shepherd[count.index].id)[1]
  query     = format("select from_unixtime(min(hour)) as min_hour, from_unixtime(max(hour)) as max_hour from \"%s\".\"%s\"", split(":", aws_glue_catalog_database.shepherd[count.index].id)[1], local.table_name)
}

resource "aws_athena_named_query" "num_records" {
  count = length(var.subscriber_buckets)

  name      = format("%s-%s-num-records", local.glue_database_name_prefix, var.subscriber_buckets[count.index])
  workgroup = aws_athena_workgroup.shepherd[count.index].id
  database  = split(":", aws_glue_catalog_database.shepherd[count.index].id)[1]
  query     = format("select count(*) from \"%s\".\"%s\"", split(":", aws_glue_catalog_database.shepherd[count.index].id)[1], local.table_name)
}

# Policy Freqs last 720 hours HHS
resource "aws_athena_named_query" "hhs-policy-freq-720" {
  count       = length(var.subscriber_buckets)
  name        = format("%s-%s-hhs-policy-freq", local.glue_database_name_prefix, var.subscriber_buckets[count.index])
  description = "Policy Freqs last 720 hours HHS"
  workgroup   = aws_athena_workgroup.shepherd[count.index].id
  database    = split(":", aws_glue_catalog_database.shepherd[count.index].id)[1]
  query       = <<-EOT
        select policy, count(*) as freq from
        (select array_join(parent_policies, ' ') AS policy
        from shepherd_global_database_sub_hhs_secops_f23sihm4.dns_data
        where subscriber='sub.hhs.secops'
        and hour >= (cast(to_unixtime(now()) as integer) -  (60 * 60 * 24 * 3))
        and parent_policies is not null
        )

        where policy in ('sb-malware-infections-block', 'sb-infected-page', 'sb-phishing-page', 'sb-safe-search', 'sb-safe-search-youtube', 'sb-restricted-schedule', 'sb-whitelist')
        group by policy
        order by freq desc;
    EOT
}

# HHS actionable last 720 hours
resource "aws_athena_named_query" "hhs-actionable-720" {
  count       = length(var.subscriber_buckets)
  name        = format("%s-%s-hhs-actionable", local.glue_database_name_prefix, var.subscriber_buckets[count.index])
  description = "Actionable HHS last 720 hours"
  workgroup   = aws_athena_workgroup.shepherd[count.index].id
  database    = split(":", aws_glue_catalog_database.shepherd[count.index].id)[1]
  query       = <<-EOT
  select client_address, policy, datetime from
  (SELECT
  client_address, from_unixtime("start_time"*0.000001)  as datetime, policy
  from shepherd_global_database_sub_hhs_secops_f23sihm4.dns_data
  CROSS JOIN UNNEST(parent_policies) AS t (policy)
  where rec_type='base'
  and subscriber='sub.hhs.secops'
  and parent_policies is not null
  and to_unixtime(now())-(start_time *0.000001) <= 60*60*720
  )
  where policy = 'sb-malware-infections-block'
  order by datetime, policy
    EOT
}

# HHS interesting last 720 hours
resource "aws_athena_named_query" "hhs-interesting-720" {
  count       = length(var.subscriber_buckets)
  name        = format("%s-%s-hhs-interesting", local.glue_database_name_prefix, var.subscriber_buckets[count.index])
  description = "Actionable HHS last 720 hours"
  workgroup   = aws_athena_workgroup.shepherd[count.index].id
  database    = split(":", aws_glue_catalog_database.shepherd[count.index].id)[1]
  query       = <<-EOT
  select client_address, policy, datetime from
  (SELECT
  client_address, from_unixtime("start_time"*0.000001)  as datetime, policy
  from shepherd_global_database_sub_hhs_secops_f23sihm4.dns_data
  CROSS JOIN UNNEST(parent_policies) AS t (policy)
  where rec_type='base'
  and subscriber='sub.hhs.secops'
  and parent_policies is not null
  and to_unixtime(now())-(start_time *0.000001) <= 60*60*720
  )
  where policy in ('sb-infected-page', 'sb-phishing-page', 'sb-safe-search', 'sb-safe-search-youtube', 'sb-restricted-schedule', 'sb-whitelist')
  order by datetime, policy
    EOT
}
