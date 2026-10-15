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
