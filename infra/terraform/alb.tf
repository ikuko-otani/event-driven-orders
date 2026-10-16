# The way in from the internet: a load balancer in the public subnets,
# forwarding HTTP to the order-api tasks.

# One load balancer across both AZs, reachable only from allowed_cidrs
# through its security group
resource "aws_lb" "main" {
  name               = var.project
  load_balancer_type = "application"
  internal           = false
  security_groups    = [aws_security_group.alb.id]
  subnets            = aws_subnet.public[*].id
}

# Where the load balancer sends requests: the tasks' own IPs on the
# app's port, since a Fargate task has no instance to register
resource "aws_lb_target_group" "api" {
  name        = "${var.project}-api"
  port        = 8000
  protocol    = "HTTP"
  target_type = "ip"
  vpc_id      = aws_vpc.main.id

  # A task leaving is drained for 30 s instead of the default 300 s,
  # so a redeploy or a destroy does not stall on idle connections
  deregistration_delay = 30

  # A task takes traffic only once the app answers /health
  health_check {
    path    = "/health"
    matcher = "200"
  }
}
