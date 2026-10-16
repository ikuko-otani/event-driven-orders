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

# The instance itself: the smallest class, a single AZ, and nothing
# left behind on destroy
resource "aws_db_instance" "main" {
  identifier     = var.project
  engine         = "postgres"
  engine_version = "18"
  instance_class = "db.t4g.micro"
  multi_az       = false

  # 20 GB is the gp3 minimum, far beyond what a demo writes; encryption
  # at rest costs nothing extra
  allocated_storage = 20
  storage_type      = "gp3"
  storage_encrypted = true

  # The database and user Compose uses, so the app's defaults still hold;
  # RDS generates the password and keeps it in Secrets Manager, so it
  # never appears in the state file
  db_name                     = "event_driven_orders"
  username                    = "app"
  manage_master_user_password = true

  # Reachable only inside the VPC, and only through the database group
  db_subnet_group_name   = aws_db_subnet_group.main.name
  vpc_security_group_ids = [aws_security_group.db.id]
  publicly_accessible    = false

  # Disposable by design: no backups to keep, no final snapshot to
  # leave behind, nothing to stop a destroy
  backup_retention_period = 0
  skip_final_snapshot     = true
  deletion_protection     = false
}
