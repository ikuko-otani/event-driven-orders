# Values read after an apply: where the deployment answers, where its
# image goes, and what run-task needs to start a one-shot job.

# The public address; the order API answers on plain HTTP here
output "alb_dns_name" {
  description = "DNS name of the load balancer, the order API's public address."
  value       = aws_lb.main.dns_name
}

# Where docker tag and push send the image; the tag is added per deploy
output "ecr_repository_url" {
  description = "URL of the order-api ECR repository, without a tag."
  value       = aws_ecr_repository.api.repository_url
}
