
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
