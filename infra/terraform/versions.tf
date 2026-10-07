# Versions this configuration is written against.
terraform {
  # The CLI: any 1.x from 1.16 on, never a new major version unseen
  required_version = "~> 1.16"

  # The AWS provider: any 6.x from 6.67 on, for the same reason
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.67"
    }
  }

  # No backend block: state stays a local, uncommitted file until the
  # deployment decides where shared state lives.
}
