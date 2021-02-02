
output "shepherd_users_role_arn" {
  value       = aws_iam_role.shepherd_users.arn
  description = "shepherd-users role arn"
}

output "shepherd_glue_role_arn" {
  value       = aws_iam_role.glue_role.arn
  description = "shepherd glue role arn"
}
