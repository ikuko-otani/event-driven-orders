# Two AZs, each with a public subnet (load balancer, tasks) and a
# private subnet (database). No NAT gateway: tasks take public IPs
# instead, and the private subnets get no route out at all.

# The region's ordinary AZs; Local Zones are left out, since neither
# a load balancer nor a database subnet group can use them
data "aws_availability_zones" "available" {
  state = "available"

  filter {
    name   = "opt-in-status"
    values = ["opt-in-not-required"]
  }
}

# One private address range for everything this deployment owns
resource "aws_vpc" "main" {
  cidr_block = "10.0.0.0/16"

  tags = {
    Name = var.project
  }
}
