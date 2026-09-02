output "postgres_internal_host" {
  description = "Host interno do PostgreSQL de destino (DNS do Service) para uso no Meltano/DAGs"
  value       = module.postgres.internal_host
}

output "postgres_port_forward" {
  description = "Comando para acessar o PostgreSQL de destino localmente"
  value       = "kubectl port-forward -n ${module.postgres.namespace} svc/${module.postgres.service_name} 5432:5432"
}

output "airflow_webserver_port_forward" {
  description = "Comando para acessar a UI do Airflow localmente (Airflow 3.x: componente api-server)"
  value       = "kubectl port-forward -n ${module.airflow.namespace} svc/airflow-api-server 8080:8080"
}
