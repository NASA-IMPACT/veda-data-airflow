
resource "aws_cloudwatch_log_group" "ecs_logs" {
  name = "${var.prefix}-stac_tasks"

  tags = {
    Environment = var.stage
    Application = var.prefix
  }
}


resource "aws_cloudwatch_log_stream" "veda_build_stac_stream" {
  name           = "ecs"
  log_group_name = aws_cloudwatch_log_group.ecs_logs.name
}