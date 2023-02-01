
resource "aws_ecs_cluster" "mwaa_cluster" {
  name = "${var.prefix}-cluster"
}

resource "aws_ecs_task_definition" "veda_task_definition" {
  container_definitions = jsonencode([
    {
      name      = "${var.prefix}-veda-stac-build"
      image     = var.docker_image_url
      essential = true,
      logConfiguration = {
        "logDriver": "awslogs",
        "options": {
          "awslogs-group": aws_cloudwatch_log_group.ecs_logs.name,
            "awslogs-region": var.aws_region,
            "awslogs-stream-prefix": aws_cloudwatch_log_stream.veda_build_stac_stream.name
          }
      }
    }
  ])
  family                   = "${var.prefix}-veda-tasks"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = 1024
  memory                   = 2048
  execution_role_arn = var.mwaa_execution_role_arn
  task_role_arn = var.mwaa_task_role_arn
}