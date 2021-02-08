
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
| aws | n/a |
| template | n/a |

## Inputs

| Name | Description | Type | Default | Required |
|------|-------------|------|---------|:--------:|
| application | n/a | `string` | `"shepherd"` | no |
| csv\_jobs | Details for each CSV job. See comments in code for details | `list(map(string))` | `[]` | no |
| domain | Top Level Domain for serving CSV results. | `string` | `""` | no |
| environment | n/a | `string` | `"global"` | no |
| project | n/a | `string` | `"shepherd"` | no |
| region | n/a | `string` | `"us-gov-west-1"` | no |
| shepherd\_users | The set of IAM user names to add to the 'shepherd\_users' group | `list(string)` | `[]` | no |
| subscriber\_buckets | The set of AWS S3 buckets to subscribe too | `list(string)` | `[]` | no |
| tags | The tags for the project | `map(string)` | `{}` | no |

## Outputs

| Name | Description |
|------|-------------|
| shepherd\_glue\_role\_arn | shepherd glue role arn |
| shepherd\_users\_role\_arn | shepherd-users role arn |

