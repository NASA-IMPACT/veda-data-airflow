output "airflow_url" {
  value = "https://${module.mwaa.airflow_url}"
}

output "mwaa_s3_name" {
  value = module.mwaa.mwaa_s3_name
}

output "mwaa_subnets" {
  value = module.mwaa.subnets
}