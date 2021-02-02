/*
 *
 * # App Shepherd Global
 *
 * This module is used to configure AWS resources to work with the Shepherd project.
 *
 * ## ETL Pipeline
 *
 * ![etl-pipeline](./images/etlpipeline.png)
 *
 * ## Usage
 *
 * Creates metric alarms for use with a Lambda Function
 *
 * * Success rate
 *
 * ```hcl
 * module "shepherd" {
 *   source = "dod-iac/shepherd/aws"
 *
 *   subscriber_buckets = [
 *     "bucket1",
 *     "bucket2",
 *   ]
 *
 *   shepherd_users = [
 *     "iam_user1",
 *     "iam_user2",
 *   ]
 *
 *   tags = {
 *     Project     = var.project
 *     Application = var.application
 *     Environment = var.environment
 *     Automation  = "Terraform"
 *   }
 * }
 * ```
 *
 * ## Terraform Version
 *
 * Terraform 0.13. Pin module version to ~> 1.0.0 . Submit pull-requests to master branch.
 *
 * Terraform 0.11 and 0.12 are not supported.
 *
 * ## License
 *
 * This project constitutes a work of the United States Government and is not subject to domestic copyright protection under 17 USC § 105.  However, because the project utilizes code licensed from contributors and other third parties, it therefore is licensed under the MIT License.  See LICENSE file for more information.
 *
 * ## Manual Operations Log
 *
 * ### Athena Workgroups
 *
 * For the Athena Workgroups it is required that the options "Queries with requester pays buckets" is set to "Enabled". Ensure that both the Athena `primary` workgroup and the Shepherd workgroups have this enabled. This will have to be done manually for any new workgroups added.
 *
 * ### Create the Glue Tables
 *
 * Each database needs a table with the data. There is a saved query in each workgroup for creating the table. After switching workgroups, and while checking the correct DB is selected, run the `create-table` query. This needs to be done for each database, remembering to switch workgroups each time. Confirm that the tables exist by looking in AWS Glue or in AWS Athena by selecting the appropriate database.
 *
 * ### AWS IAM Roles
 *
 * There are two roles that must be passed to the vendor and appear as outputs:
 *
 * * shepherd_glue_role_arn: The role used by AWS Glue to do ETL on the data
 * * shepherd_users_role_arn: The role used by IAM users to work with the resources configured by this module
 */
data "aws_caller_identity" "current" {}
data "aws_iam_account_alias" "current" {}
data "aws_region" "current" {}
data "aws_partition" "current" {}
