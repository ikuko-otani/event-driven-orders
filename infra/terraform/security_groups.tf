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
