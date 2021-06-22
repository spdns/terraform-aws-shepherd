
resource "aws_iam_role" "shepherd_users" {
  name                 = "shepherd_users"
  max_session_duration = 36000
  description          = "Role for 'shepherd_users' to use"
  assume_role_policy   = data.aws_iam_policy_document.assume_role_policy.json
}

resource "aws_iam_role" "shepherd_engineers" {
  name               = "shepherd_engineers"
  description        = "Role for 'shepherd_engineers' to use"
  assume_role_policy = data.aws_iam_policy_document.assume_role_policy.json
}

resource "aws_iam_role" "shepherd_redshift" {
  name               = "shepherd_redshift"
  description        = "Role for `shepherd_redshift` to use as a demo"
  assume_role_policy = data.aws_iam_policy_document.assume_redshift_role_policy.json
}