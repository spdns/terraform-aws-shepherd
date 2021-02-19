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
 *     Name           = "" // A unique name, no spaces only dashes
 *     Bucket         = "" // The subscriber bucket
 *     Ordinal        = "" // An ordinal unique to this job. Modifying will change bucket location.
 *     Subscriber     = "" // The name of the subscriber, ie sub.dod.dds. Modifying will change bucket location.
 *     Receiver       = "" // Comma separated point-of-contact emails. Modifying will change bucket location.
 *     Policies       = "" // Comma separated list of policies for job filtering
 *     HoursAgo       = "" // The number of hours ago for the data to process
 *     OutputFilename = "" // The final name of the fil
 *   }
 * ]
 */
variable "csv_jobs" {
  type        = list(map(string))
  default     = []
  description = "Details for each CSV job. See comments in code for details"
}

variable "csv_bucket_name" {
  type        = string
  default     = ""
  description = "The name of the S3 bucket hosting the publicly accessible CSV files. The name must be a valid DNS name. Best practice is to use a unique hash in the name, ie UNIQUEHASH.example.com"
}

variable "csv_bucket_allowed_ip_blocks" {
  type        = list(string)
  default     = ["0.0.0.0/0"]
  description = "List of CIDR blocks allowed to access the CSV bucket"
}

locals {
  project_tags = merge({
    Project     = var.project
    Application = var.application
    Environment = var.environment
    Automation  = "Terraform"
  }, var.tags)
}
