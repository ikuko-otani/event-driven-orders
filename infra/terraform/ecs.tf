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

# The order-api container; the image's own CMD starts uvicorn
locals {
  api_container = {
    name             = "api"
    image            = local.image
    portMappings     = [{ containerPort = 8000, protocol = "tcp" }]
    logConfiguration = local.log_configuration

    # Redis shares the task's network, so it is on localhost; the
    # database values are the ones every task here uses
    environment = concat(local.db_environment, [{ name = "REDIS_HOST", value = "localhost" }])
    secrets     = local.db_secrets

    # Started after Redis and stopped before it, so no request meets a
    # cache that is not there yet, or no longer is
    dependsOn = [{ containerName = "redis", condition = "START" }]

    # Ten seconds from SIGTERM to SIGKILL, written down rather than
    # left to the 30 s default
    stopTimeout = 10
  }
}

# The response cache beside the app (design §4.4). Only a cache: the
# database's unique constraint stays the authority, so losing it with
# the task loses nothing that matters
locals {
  redis_container = {
    name             = "redis"
    logConfiguration = local.log_configuration
    stopTimeout      = 10

    # The tag Compose runs, from AWS's mirror of the official images,
    # which keeps pulls clear of Docker Hub's anonymous rate limit
    image = "public.ecr.aws/docker/library/redis:8.10.1-alpine"
  }
}

# The task the service keeps running: the app and its cache together,
# on the smallest size Fargate offers
resource "aws_ecs_task_definition" "api" {
  family                   = "${var.project}-api"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = "256"
  memory                   = "512"

  # The image is built on an x86 machine; said explicitly, so a change
  # of default can never start it on ARM
  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = "X86_64"
  }

  # ECS starts the task under this role; the app itself gets none
  execution_role_arn = aws_iam_role.task_execution.arn

  container_definitions = jsonencode([local.api_container, local.redis_container])
}

# The jobs a deploy runs once, before the app is first used: the
# schema, then the master data. Each gets a task definition of its own,
# so run-task needs no command override. Only the orders schema:
# inventory-worker is not deployed
locals {
  oneshot_commands = {
    migrate = [
      "uv", "run", "--no-sync", "--package", "order-api",
      "alembic", "-c", "services/order-api/alembic.ini", "upgrade", "head",
    ]
    seed = [
      "uv", "run", "--no-sync", "--package", "order-api",
      "python", "scripts/seed_orders.py",
    ]
  }
}

# Each job's container: the app's image with the job's command, and
# the same database values the app uses. No Redis beside it: neither
# job serves a request, so neither reads the cache
locals {
  oneshot_containers = {
    for name, command in local.oneshot_commands : name => {
      name             = name
      image            = local.image
      command          = command
      environment      = local.db_environment
      secrets          = local.db_secrets
      logConfiguration = local.log_configuration
      stopTimeout      = 10
    }
  }
}
