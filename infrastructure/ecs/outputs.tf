output "cluster_name" {
  value = aws_ecs_cluster.mwaa_cluster.name
}

output "log_group_name" {
  value = aws_cloudwatch_log_group.ecs_logs.name
}

output "stream_log_name" {
  value = aws_cloudwatch_log_stream.veda_build_stac_stream.name
}