# Frankfurt by default; the whole deployment lives in one region
variable "aws_region" {
  description = "AWS region every resource is created in."
  type        = string
  default     = "eu-central-1"
}

# One name for resource names and the Project tag, so they never drift apart
variable "project" {
  description = "Project name, used in resource names and the Project tag."
  type        = string
  default     = "event-driven-orders"
}
