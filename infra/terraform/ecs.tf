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

# Values every task definition in this file shares, so the app and its
# one-shot tasks can never reach the database differently
locals {
  # The image pushed for this deploy, named by the tag the apply was given
  image = "${aws_ecr_repository.api.repository_url}:${var.image_tag}"

  # Read off the instance itself rather than repeated, so a renamed
  # database or user cannot leave the tasks pointing at the old one
  db_environment = [
    { name = "DB_HOST", value = aws_db_instance.main.address },
    { name = "DB_PORT", value = tostring(aws_db_instance.main.port) },
    { name = "DB_NAME", value = aws_db_instance.main.db_name },
    { name = "DB_USER", value = aws_db_instance.main.username },
  ]

  # The password comes from the secret RDS manages: ECS reads it at
  # start-up, so it never appears in the task definition or the state
  db_secrets = [{
    name      = "DB_PASSWORD"
    valueFrom = "${aws_db_instance.main.master_user_secret[0].secret_arn}:password::"
  }]
}

# How every container in this file logs: all into the one group, where
# ECS names each stream prefix/container/task, so the container name
# already tells them apart
locals {
  log_configuration = {
    logDriver = "awslogs"
    options = {
      "awslogs-group"         = aws_cloudwatch_log_group.ecs.name
      "awslogs-region"        = var.aws_region
      "awslogs-stream-prefix" = var.project
    }
  }
}
