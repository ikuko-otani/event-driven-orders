# Where the order-api tasks run. Task definitions and the service join
# this file as they are written.

# The cluster is only a name to group tasks under; Fargate supplies the
# machines, so there is nothing here to size
resource "aws_ecs_cluster" "main" {
  name = var.project

  # Off whatever the account default says: Container Insights is billed
  # per metric, and the log group already holds what a demo needs
  setting {
    name  = "containerInsights"
    value = "disabled"
  }
}
