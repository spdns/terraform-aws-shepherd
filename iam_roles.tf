
resource "aws_iam_role" "shepherd_users" {
  name               = "shepherd_users"
  description        = "Role for 'shepherd_users' to use"
  assume_role_policy = data.aws_iam_policy_document.assume_role_policy.json
}