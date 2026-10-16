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
