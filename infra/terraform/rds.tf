# The database: one small PostgreSQL instance in the private subnets.

# Where the instance may be placed; RDS demands two AZs here even for a
# single instance, so a failover always has somewhere to go
resource "aws_db_subnet_group" "main" {
  name       = var.project
  subnet_ids = aws_subnet.private[*].id

  tags = {
    Name = var.project
  }
}
