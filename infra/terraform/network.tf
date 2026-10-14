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

# Public half, one per AZ: the load balancer and the tasks
resource "aws_subnet" "public" {
  count = 2

  vpc_id            = aws_vpc.main.id
  availability_zone = data.aws_availability_zones.available.names[count.index]
  cidr_block        = cidrsubnet(aws_vpc.main.cidr_block, 8, count.index)

  tags = {
    Name = "${var.project}-public-${count.index}"
  }
}

# Private half, one per AZ: the database only. Numbered from 10, so a
# third AZ's public subnet never collides with them
resource "aws_subnet" "private" {
  count = 2

  vpc_id            = aws_vpc.main.id
  availability_zone = data.aws_availability_zones.available.names[count.index]
  cidr_block        = cidrsubnet(aws_vpc.main.cidr_block, 8, count.index + 10)

  tags = {
    Name = "${var.project}-private-${count.index}"
  }
}
