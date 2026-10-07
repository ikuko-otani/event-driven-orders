# Credentials are never written here: the provider reads them from the
# environment (AWS_PROFILE pointing at a short-lived SSO session).
provider "aws" {
  region = var.aws_region

  # Stamp every resource, so what this configuration owns can be found
  # by tag, and confirmed gone after a destroy.
  default_tags {
    tags = {
      Project   = var.project
      ManagedBy = "terraform"
    }
  }
}
