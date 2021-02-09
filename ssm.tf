
data "aws_ssm_parameter" "salt" {
  name = format("/%s-%s/salt", var.project, var.environment)
}
