
resource "aws_iam_group" "shepherd_users" {
  name = "shepherd_users"
}

// Using a data resource validates that the users exist before applying
data "aws_iam_user" "shepherd_users" {
  count     = length(var.shepherd_users)
  user_name = var.shepherd_users[count.index]
}

resource "aws_iam_group_membership" "shepherd_users" {
  count = length(var.shepherd_users) > 0 ? 1 : 0

  name  = "shepherd_user_group_membership"
  group = aws_iam_group.shepherd_users.name
  users = data.aws_iam_user.shepherd_users[*].user_name
}


resource "aws_iam_group" "shepherd_engineers" {
  name = "shepherd_engineers"
}

// Using a data resource validates that the engineers exist before applying
data "aws_iam_user" "shepherd_engineers" {
  count     = length(var.shepherd_engineers)
  user_name = var.shepherd_engineers[count.index]
}

resource "aws_iam_group_membership" "shepherd_engineers" {
  count = length(var.shepherd_engineers) > 0 ? 1 : 0

  name  = "shepherd_user_group_membership"
  group = aws_iam_group.shepherd_engineers.name
  users = data.aws_iam_user.shepherd_engineers[*].user_name
}

// Adding this for the redshift demo
resource "aws_iam_group" "shepherd_redshift" {
  name = "shepherd_redshift"
}