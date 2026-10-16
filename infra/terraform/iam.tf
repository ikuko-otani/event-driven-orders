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
