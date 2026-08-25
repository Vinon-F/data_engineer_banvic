output "minio_endpoint" {
  description = "Endpoint interno do MinIO (S3-compatible) para uso no Meltano/DAGs"
  value       = module.minio.internal_endpoint
}

output "minio_console_port_forward" {
  description = "Comando para acessar o console do MinIO localmente"
  value       = "kubectl port-forward -n ${module.minio.namespace} svc/minio-console 9001:9001"
}

output "airflow_webserver_port_forward" {
  description = "Comando para acessar a UI do Airflow localmente (Airflow 3.x: componente api-server)"
  value       = "kubectl port-forward -n ${module.airflow.namespace} svc/airflow-api-server 8080:8080"
}
