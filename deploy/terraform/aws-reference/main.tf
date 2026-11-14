# This module establishes an artifact boundary and empty credential containers.
# It creates no cluster/database and never stores credential values in state.
resource "aws_ecr_repository" "atlas" {
  name                 = var.name
  image_tag_mutability = "IMMUTABLE"
  image_scanning_configuration {
    scan_on_push = true
  }
}
resource "aws_secretsmanager_secret" "database" {
  for_each                = toset(["api", "worker", "provider", "migration"])
  name                    = "${var.name}/${each.key}/database"
  recovery_window_in_days = 30
}
resource "aws_cloudwatch_log_group" "atlas" {
  name              = "/atlas/${var.name}"
  retention_in_days = 30
}
output "image_repository" {
  value = aws_ecr_repository.atlas.repository_url
}
output "credential_container_arns" {
  value = { for service, secret in aws_secretsmanager_secret.database : service => secret.arn }
}
