
# App Shepherd Global

This module is used to configure AWS resources to work with the Shepherd project.

## ETL Pipeline

![etl-pipeline](./images/etlpipeline.png)

## Usage

Creates metric alarms for use with a Lambda Function

* Success rate

```hcl
module "shepherd" {
  source = "dod-iac/shepherd/aws"

  subscriber_buckets = [
    "bucket1",
    "bucket2",
  ]

  shepherd_users = [
    "iam_user1",
    "iam_user2",
  ]

  tags = {
    Project     = var.project
    Application = var.application
    Environment = var.environment
    Automation  = "Terraform"
  }
}
```

## Terraform Version

Terraform 0.13. Pin module version to ~> 1.0.0 . Submit pull-requests to master branch.

Terraform 0.11 and 0.12 are not supported.

## License

This project constitutes a work of the United States Government and is not subject to domestic copyright protection under 17 USC § 105.  However, because the project utilizes code licensed from contributors and other third parties, it therefore is licensed under the MIT License.  See LICENSE file for more information.

## Manual Operations Log

### Athena Workgroups

For the Athena Workgroups it is required that the options "Queries with requester pays buckets" is set to "Enabled". Ensure that both the Athena `primary` workgroup and the Shepherd workgroups have this enabled. This will have to be done manually for any new workgroups added.

### Create the Glue Tables

Each database needs a table with the data. There is a saved query in each workgroup for creating the table. After switching workgroups, and while checking the correct DB is selected, run the `create-table` query. This needs to be done for each database, remembering to switch workgroups each time. Confirm that the tables exist by looking in AWS Glue or in AWS Athena by selecting the appropriate database.

### AWS IAM Roles

There are two roles that must be passed to the vendor and appear as outputs:

* shepherd\_glue\_role\_arn: The role used by AWS Glue to do ETL on the data
* shepherd\_users\_role\_arn: The role used by IAM users to work with the resources configured by this module

### AWS SSM Parameters

Some data needs to be placed in AWS SSM Parameter store. They are:

* `salt`: A random 32 character string used as a salt for hashing algorithms

To write a variable use the [chamber](https://github.com/segmentio/chamber) tool:

```sh
SALT=$(tr -dc 'a-zA-Z0-9' < /dev/urandom | fold -w 32 | head -n 1)
chamber write shepherd-global salt "${SALT}"
```

## Requirements

No requirements.

## Providers

| Name | Version |
|------|---------|
| <a name="provider_aws"></a> [aws](#provider\_aws) | n/a |
| <a name="provider_template"></a> [template](#provider\_template) | n/a |

## Modules

| Name | Source | Version |
|------|--------|---------|
| <a name="module_athena_results"></a> [athena\_results](#module\_athena\_results) | trussworks/s3-private-bucket/aws | ~> 3.2.1 |
| <a name="module_aws_logs"></a> [aws\_logs](#module\_aws\_logs) | trussworks/logs/aws | ~> 10.0.0 |
| <a name="module_glue_tmp_bucket"></a> [glue\_tmp\_bucket](#module\_glue\_tmp\_bucket) | trussworks/s3-private-bucket/aws | ~> 3.2.1 |

## Resources

| Name | Type |
|------|------|
| [aws_athena_named_query.alter_table](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/athena_named_query) | resource |
| [aws_athena_named_query.create_proxy_table](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/athena_named_query) | resource |
| [aws_athena_named_query.create_table](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/athena_named_query) | resource |
| [aws_athena_named_query.create_view](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/athena_named_query) | resource |
| [aws_athena_named_query.create_view_proxy](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/athena_named_query) | resource |
| [aws_athena_named_query.repair_table](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/athena_named_query) | resource |
| [aws_athena_workgroup.shepherd](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/athena_workgroup) | resource |
| [aws_glue_catalog_database.shepherd](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/glue_catalog_database) | resource |
| [aws_glue_job.create_csv](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/glue_job) | resource |
| [aws_glue_job.shepherd](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/glue_job) | resource |
| [aws_glue_job.shepherd_proxy](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/glue_job) | resource |
| [aws_glue_security_configuration.event_data](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/glue_security_configuration) | resource |
| [aws_glue_trigger.start_workflow_create_csv](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/glue_trigger) | resource |
| [aws_glue_trigger.start_workflow_loadpartitions](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/glue_trigger) | resource |
| [aws_glue_trigger.start_workflow_loadpartitions_proxy](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/glue_trigger) | resource |
| [aws_iam_group.shepherd_engineers](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/iam_group) | resource |
| [aws_iam_group.shepherd_redshift](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/iam_group) | resource |
| [aws_iam_group.shepherd_users](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/iam_group) | resource |
| [aws_iam_group_membership.shepherd_engineers](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/iam_group_membership) | resource |
| [aws_iam_group_membership.shepherd_users](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/iam_group_membership) | resource |
| [aws_iam_group_policy_attachment.shepherd_engineers_assume_role_policy_attachment](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/iam_group_policy_attachment) | resource |
| [aws_iam_group_policy_attachment.shepherd_redshift_assume_role_policy_attachment](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/iam_group_policy_attachment) | resource |
| [aws_iam_group_policy_attachment.shepherd_users_assume_role_policy_attachment](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/iam_group_policy_attachment) | resource |
| [aws_iam_policy.assume_role_shepherd_engineers_policy](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/iam_policy) | resource |
| [aws_iam_policy.assume_role_shepherd_redshift_policy](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/iam_policy) | resource |
| [aws_iam_policy.assume_role_shepherd_users_policy](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/iam_policy) | resource |
| [aws_iam_policy.glue_policy](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/iam_policy) | resource |
| [aws_iam_policy.quicksight_service](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/iam_policy) | resource |
| [aws_iam_policy.shepherd_athena_primarywg](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/iam_policy) | resource |
| [aws_iam_policy.shepherd_engineers](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/iam_policy) | resource |
| [aws_iam_policy.shepherd_redshift_athena](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/iam_policy) | resource |
| [aws_iam_policy.shepherd_redshift_glue](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/iam_policy) | resource |
| [aws_iam_policy.shepherd_redshift_kms](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/iam_policy) | resource |
| [aws_iam_policy.shepherd_redshift_s3](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/iam_policy) | resource |
| [aws_iam_policy.shepherd_users_athena](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/iam_policy) | resource |
| [aws_iam_policy.shepherd_users_glue](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/iam_policy) | resource |
| [aws_iam_policy.shepherd_users_other](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/iam_policy) | resource |
| [aws_iam_policy.shepherd_users_s3](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/iam_policy) | resource |
| [aws_iam_role.glue_role](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/iam_role) | resource |
| [aws_iam_role.quicksight_service](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/iam_role) | resource |
| [aws_iam_role.shepherd_engineers](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/iam_role) | resource |
| [aws_iam_role.shepherd_redshift](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/iam_role) | resource |
| [aws_iam_role.shepherd_users](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/iam_role) | resource |
| [aws_iam_role_policy_attachment.glue_policy_att](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/iam_role_policy_attachment) | resource |
| [aws_iam_role_policy_attachment.glue_role_att](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/iam_role_policy_attachment) | resource |
| [aws_iam_role_policy_attachment.quicksight_athena_policy_att](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/iam_role_policy_attachment) | resource |
| [aws_iam_role_policy_attachment.quicksight_service_policy_attachment](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/iam_role_policy_attachment) | resource |
| [aws_iam_role_policy_attachment.shepherd_engineers_policy_attachment](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/iam_role_policy_attachment) | resource |
| [aws_iam_role_policy_attachment.shepherd_engineers_policy_attachment_primarywg](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/iam_role_policy_attachment) | resource |
| [aws_iam_role_policy_attachment.shepherd_redshift](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/iam_role_policy_attachment) | resource |
| [aws_iam_role_policy_attachment.shepherd_redshift_policy_attachment_athena](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/iam_role_policy_attachment) | resource |
| [aws_iam_role_policy_attachment.shepherd_redshift_policy_attachment_glue](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/iam_role_policy_attachment) | resource |
| [aws_iam_role_policy_attachment.shepherd_redshift_policy_attachment_kms](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/iam_role_policy_attachment) | resource |
| [aws_iam_role_policy_attachment.shepherd_redshift_policy_attachment_s3](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/iam_role_policy_attachment) | resource |
| [aws_iam_role_policy_attachment.shepherd_users_policy_attachment_athena](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/iam_role_policy_attachment) | resource |
| [aws_iam_role_policy_attachment.shepherd_users_policy_attachment_glue](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/iam_role_policy_attachment) | resource |
| [aws_iam_role_policy_attachment.shepherd_users_policy_attachment_other](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/iam_role_policy_attachment) | resource |
| [aws_iam_role_policy_attachment.shepherd_users_policy_attachment_primarywg](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/iam_role_policy_attachment) | resource |
| [aws_iam_role_policy_attachment.shepherd_users_policy_attachment_s3](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/iam_role_policy_attachment) | resource |
| [aws_s3_bucket.csv_results](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/s3_bucket) | resource |
| [aws_s3_bucket_object.create_csv](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/s3_bucket_object) | resource |
| [aws_s3_bucket_object.index-html](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/s3_bucket_object) | resource |
| [aws_s3_bucket_object.loadpartition](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/s3_bucket_object) | resource |
| [aws_s3_bucket_object.not-found-html](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/s3_bucket_object) | resource |
| [aws_caller_identity.current](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/data-sources/caller_identity) | data source |
| [aws_iam_account_alias.current](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/data-sources/iam_account_alias) | data source |
| [aws_iam_policy_document.assume_redshift_role_policy](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/data-sources/iam_policy_document) | data source |
| [aws_iam_policy_document.assume_role_policy](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/data-sources/iam_policy_document) | data source |
| [aws_iam_policy_document.assume_role_shepherd_engineers_policy_doc](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/data-sources/iam_policy_document) | data source |
| [aws_iam_policy_document.assume_role_shepherd_redshift_policy_doc](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/data-sources/iam_policy_document) | data source |
| [aws_iam_policy_document.assume_role_shepherd_users_policy_doc](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/data-sources/iam_policy_document) | data source |
| [aws_iam_policy_document.csv_results_policy](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/data-sources/iam_policy_document) | data source |
| [aws_iam_policy_document.glue_assume_role_policy_document](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/data-sources/iam_policy_document) | data source |
| [aws_iam_policy_document.glue_policy_document](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/data-sources/iam_policy_document) | data source |
| [aws_iam_policy_document.quicksight_assume_role_policy](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/data-sources/iam_policy_document) | data source |
| [aws_iam_policy_document.quicksight_service](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/data-sources/iam_policy_document) | data source |
| [aws_iam_policy_document.shepherd_athena_primarywg](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/data-sources/iam_policy_document) | data source |
| [aws_iam_policy_document.shepherd_engineers](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/data-sources/iam_policy_document) | data source |
| [aws_iam_policy_document.shepherd_redshift_athena](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/data-sources/iam_policy_document) | data source |
| [aws_iam_policy_document.shepherd_redshift_glue](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/data-sources/iam_policy_document) | data source |
| [aws_iam_policy_document.shepherd_redshift_kms](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/data-sources/iam_policy_document) | data source |
| [aws_iam_policy_document.shepherd_redshift_s3](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/data-sources/iam_policy_document) | data source |
| [aws_iam_policy_document.shepherd_users_athena](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/data-sources/iam_policy_document) | data source |
| [aws_iam_policy_document.shepherd_users_glue](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/data-sources/iam_policy_document) | data source |
| [aws_iam_policy_document.shepherd_users_other](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/data-sources/iam_policy_document) | data source |
| [aws_iam_policy_document.shepherd_users_s3](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/data-sources/iam_policy_document) | data source |
| [aws_iam_user.shepherd_engineers](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/data-sources/iam_user) | data source |
| [aws_iam_user.shepherd_users](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/data-sources/iam_user) | data source |
| [aws_partition.current](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/data-sources/partition) | data source |
| [aws_region.current](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/data-sources/region) | data source |
| [aws_ssm_parameter.salt](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/data-sources/ssm_parameter) | data source |
| [template_file.alter_table](https://registry.terraform.io/providers/hashicorp/template/latest/docs/data-sources/file) | data source |
| [template_file.create_proxy_table](https://registry.terraform.io/providers/hashicorp/template/latest/docs/data-sources/file) | data source |
| [template_file.create_table](https://registry.terraform.io/providers/hashicorp/template/latest/docs/data-sources/file) | data source |

## Inputs

| Name | Description | Type | Default | Required |
|------|-------------|------|---------|:--------:|
| <a name="input_application"></a> [application](#input\_application) | n/a | `string` | `"shepherd"` | no |
| <a name="input_csv_bucket_allowed_ip_blocks"></a> [csv\_bucket\_allowed\_ip\_blocks](#input\_csv\_bucket\_allowed\_ip\_blocks) | List of CIDR blocks allowed to access the CSV bucket | `list(string)` | <pre>[<br>  "0.0.0.0/0"<br>]</pre> | no |
| <a name="input_csv_bucket_name"></a> [csv\_bucket\_name](#input\_csv\_bucket\_name) | The name of the S3 bucket hosting the publicly accessible CSV files. The name must be a valid DNS name. Best practice is to use a unique hash in the name, ie UNIQUEHASH.example.com | `string` | `""` | no |
| <a name="input_csv_jobs"></a> [csv\_jobs](#input\_csv\_jobs) | Details for each CSV job. See comments in code for details | `list(map(string))` | `[]` | no |
| <a name="input_environment"></a> [environment](#input\_environment) | n/a | `string` | `"global"` | no |
| <a name="input_project"></a> [project](#input\_project) | n/a | `string` | `"shepherd"` | no |
| <a name="input_region"></a> [region](#input\_region) | n/a | `string` | `"us-gov-west-1"` | no |
| <a name="input_shepherd_engineers"></a> [shepherd\_engineers](#input\_shepherd\_engineers) | The set of IAM user names to add to the 'shepherd\_engineers' group | `list(string)` | `[]` | no |
| <a name="input_shepherd_users"></a> [shepherd\_users](#input\_shepherd\_users) | The set of IAM user names to add to the 'shepherd\_users' group | `list(string)` | `[]` | no |
| <a name="input_subscriber_buckets"></a> [subscriber\_buckets](#input\_subscriber\_buckets) | The set of AWS S3 buckets to subscribe too | `list(string)` | `[]` | no |
| <a name="input_tags"></a> [tags](#input\_tags) | The tags for the project | `map(string)` | `{}` | no |

## Outputs

| Name | Description |
|------|-------------|
| <a name="output_csv_results_bucket"></a> [csv\_results\_bucket](#output\_csv\_results\_bucket) | The CSV results bucket name |
| <a name="output_csv_website_domain"></a> [csv\_website\_domain](#output\_csv\_website\_domain) | The CSV domain of the website endpoint, if the bucket is configured with a website. This is used to create Route 53 alias records. |
| <a name="output_csv_website_endpoint"></a> [csv\_website\_endpoint](#output\_csv\_website\_endpoint) | The CSV website endpoint, if the bucket is configured with a website. |
| <a name="output_shepherd_glue_role_arn"></a> [shepherd\_glue\_role\_arn](#output\_shepherd\_glue\_role\_arn) | shepherd glue role arn |
| <a name="output_shepherd_users_role_arn"></a> [shepherd\_users\_role\_arn](#output\_shepherd\_users\_role\_arn) | shepherd-users role arn |
