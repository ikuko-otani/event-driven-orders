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

# The cluster and network a one-shot task joins, the same ones the
# service uses: aws ecs run-task asks for all three
output "cluster_name" {
  description = "Name of the ECS cluster."
  value       = aws_ecs_cluster.main.name
}

output "task_subnet_ids" {
  description = "Public subnets the tasks run in."
  value       = aws_subnet.public[*].id
}

output "task_security_group_id" {
  description = "Security group the tasks run under."
  value       = aws_security_group.task.id
}

# Each one-shot job's task family, keyed by job name
output "oneshot_task_families" {
  description = "Task definition family of each one-shot job, keyed by job name."
  value       = { for name, task in aws_ecs_task_definition.oneshot : name => task.family }
}
