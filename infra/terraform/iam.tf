# The one role ECS assumes to start a task: pull the image, write logs,
# read the database secret. The app itself gets no role, since it calls
# no AWS API.

# Only ECS tasks may take this role on; no person or other service can
data "aws_iam_policy_document" "task_execution_assume" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
  }
}

# The role itself, named under the project prefix that the IAM rights
# of the deploying user are scoped to
resource "aws_iam_role" "task_execution" {
  name               = "${var.project}-task-execution"
  assume_role_policy = data.aws_iam_policy_document.task_execution_assume.json
}

# What the role may do, each right narrowed to this deployment's own
# resources; the AWS managed policy would open every repository and
# every log group in the account
data "aws_iam_policy_document" "task_execution" {
  # Signing in to the registry has nothing to narrow to; AWS accepts
  # only "*" for this one action
  statement {
    actions   = ["ecr:GetAuthorizationToken"]
    resources = ["*"]
  }

  # Pull the image, from this repository only
  statement {
    actions = [
      "ecr:BatchCheckLayerAvailability",
      "ecr:GetDownloadUrlForLayer",
      "ecr:BatchGetImage",
    ]
    resources = [aws_ecr_repository.api.arn]
  }

  # Write the containers' output into this group only; the group
  # already exists, so there is no right to create one
  statement {
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["${aws_cloudwatch_log_group.ecs.arn}:*"]
  }

  # Read the password RDS generated, and no other secret
  statement {
    actions   = ["secretsmanager:GetSecretValue"]
    resources = [aws_db_instance.main.master_user_secret[0].secret_arn]
  }
}
