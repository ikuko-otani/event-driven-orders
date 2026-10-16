# Where the tasks' stdout goes: every container's JSON log lines.

# One group for the whole deployment, created here rather than by ECS,
# so destroy removes it and the tasks need no right to create one
resource "aws_cloudwatch_log_group" "ecs" {
  name = "/ecs/${var.project}"

  # A demo needs a week of history at most, and stored logs are billed
  retention_in_days = 7
}
