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

# The VPC's one door to the internet
resource "aws_internet_gateway" "main" {
  vpc_id = aws_vpc.main.id

  tags = {
    Name = var.project
  }
}

# Anything not addressed inside the VPC leaves through that door; the
# VPC's own range is routed locally without being listed here
resource "aws_route_table" "public" {
  vpc_id = aws_vpc.main.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.main.id
  }

  tags = {
    Name = "${var.project}-public"
  }
}

# A route table does nothing until a subnet is attached to it
resource "aws_route_table_association" "public" {
  count = 2

  subnet_id      = aws_subnet.public[count.index].id
  route_table_id = aws_route_table.public.id
}
