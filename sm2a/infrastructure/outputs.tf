output "Airflow_url" {
  value = module.sma-base.airflow_url
}
output "Airflow_master_secret_manager" {
  value = "Visit ${module.sma-base.airflow_secret_name} For SM2A credentials"
}
