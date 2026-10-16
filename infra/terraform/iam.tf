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
