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

# Who may reach the load balancer. No default on purpose: every plan
# and apply must say it, so the endpoint is never opened by accident.
variable "allowed_cidrs" {
  description = "CIDR blocks allowed to reach the load balancer on port 80."
  type        = list(string)

  # An empty list would plan cleanly and leave the endpoint unreachable
  validation {
    condition     = length(var.allowed_cidrs) > 0
    error_message = "Give at least one CIDR block, e.g. your own IP as x.x.x.x/32."
  }
}

# Which pushed image the tasks run. No default on purpose: a new tag is
# what makes the service roll onto a new image, so every plan and apply
# names the one it means.
variable "image_tag" {
  description = "Tag of the order-api image in ECR, e.g. the short git commit SHA."
  type        = string
}
