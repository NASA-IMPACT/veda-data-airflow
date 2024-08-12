output "airflow_url" {
  value = "https://${module.mwaa.airflow_url}"
}
output "mwaa_s3_name" {
  value = module.mwaa.mwaa_s3_name
}
output "mwaa_subnets" {
  value = module.mwaa.subnets
}
output "airflow_env" {
  value = module.mwaa.mwaa_environment_name
}
output "workflows_api" {
  value = aws_apigatewayv2_api.workflows_http_api.api_endpoint
}