# Where the order-api image is pushed, and where the tasks pull it from.

# One repository for the one image this deployment runs; the slash
# groups it under the project in the registry
resource "aws_ecr_repository" "api" {
  name = "${var.project}/api"

  # Destroy must succeed while pushed images are still inside
  force_delete = true
}
