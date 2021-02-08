
output "shepherd_users_role_arn" {
  value       = aws_iam_role.shepherd_users.arn
  description = "shepherd-users role arn"
}

output "shepherd_glue_role_arn" {
  value       = aws_iam_role.glue_role.arn
  description = "shepherd glue role arn"
}

output "csv_results_bucket" {
  value       = aws_s3_bucket.csv_results.id
  description = "The CSV results bucket name"
}

output "csv_website_endpoint" {
  value       = aws_s3_bucket.csv_results.website_endpoint
  description = "The CSV website endpoint, if the bucket is configured with a website."
}

output "csv_website_domain" {
  value       = aws_s3_bucket.csv_results.website_domain
  description = "The CSV domain of the website endpoint, if the bucket is configured with a website. This is used to create Route 53 alias records."
}