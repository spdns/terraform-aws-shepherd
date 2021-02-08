variable "region" {
  type    = string
  default = "us-gov-west-1"
}

variable "application" {
  type    = string
  default = "shepherd"
}

variable "environment" {
  type    = string
  default = "global"
}

variable "project" {
  type    = string
  default = "shepherd"
}

variable "tags" {
  type        = map(string)
  default     = {}
  description = "The tags for the project"
}

variable "shepherd_users" {
  type        = list(string)
  default     = []
  description = "The set of IAM user names to add to the 'shepherd_users' group"
}

variable "subscriber_buckets" {
  type        = list(string)
  default     = []
  description = "The set of AWS S3 buckets to subscribe too"
}

/*
 * [
 *   {
 *     Name       = "" // A unique name, no spaces only dashes
 *     Database   = "" // The database to pull from (or view?)
 *     Ordinal    = "" // An ordinal unique to this job. Modifying will change bucket location.
 *     Subscriber = "" // The name of the subscriber, ie sub.dod.dds. Modifying will change bucket location.
 *     Receiver   = "" // Comma separated point-of-contact emails. Modifying will change bucket location.
 *     Policies   = "" // Comma separated list of policies for job filtering
 *   }
 * ]
 */
variable "csv_jobs" {
  type        = list(map(string))
  default     = []
  description = "Details for each CSV job. See comments in code for details"
}

variable "domain" {
  type        = string
  default     = ""
  description = "Top Level Domain for serving CSV results."
}

locals {
  project_tags = merge({
    Project     = var.project
    Application = var.application
    Environment = var.environment
    Automation  = "Terraform"
  }, var.tags)
}
