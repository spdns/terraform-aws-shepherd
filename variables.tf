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

locals {
  project_tags = merge({
    Project     = var.project
    Application = var.application
    Environment = var.environment
    Automation  = "Terraform"
  }, var.tags)
}
