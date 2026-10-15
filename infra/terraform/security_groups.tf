# Three security groups chained by reference, never by address: the
# load balancer, the tasks behind it, and the database behind them.
# Each rule is a resource of its own, further down this file.

# The only group that faces the internet
resource "aws_security_group" "alb" {
  name        = "${var.project}-alb"
  description = "Load balancer: HTTP in from allowed_cidrs only"
  vpc_id      = aws_vpc.main.id

  tags = {
    Name = "${var.project}-alb"
  }
}

# The order-api tasks: reachable from the load balancer only
resource "aws_security_group" "task" {
  name        = "${var.project}-task"
  description = "order-api tasks: traffic from the load balancer only"
  vpc_id      = aws_vpc.main.id

  tags = {
    Name = "${var.project}-task"
  }
}

# The database: reachable from the tasks only, and with no way out
resource "aws_security_group" "db" {
  name        = "${var.project}-db"
  description = "PostgreSQL: connections from the order-api tasks only"
  vpc_id      = aws_vpc.main.id

  tags = {
    Name = "${var.project}-db"
  }
}

# HTTP from each allowed range, one rule per range, so adding or
# removing an address touches that rule alone
resource "aws_vpc_security_group_ingress_rule" "alb_http" {
  for_each = toset(var.allowed_cidrs)

  security_group_id = aws_security_group.alb.id
  description       = "HTTP from an allowed range"
  cidr_ipv4         = each.value
  ip_protocol       = "tcp"
  from_port         = 80
  to_port           = 80
}

# The load balancer talks to nothing but the tasks, on the app's port
resource "aws_vpc_security_group_egress_rule" "alb_to_task" {
  security_group_id            = aws_security_group.alb.id
  description                  = "To the order-api tasks"
  referenced_security_group_id = aws_security_group.task.id
  ip_protocol                  = "tcp"
  from_port                    = 8000
  to_port                      = 8000
}
