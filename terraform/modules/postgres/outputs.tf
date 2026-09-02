output "namespace" {
  description = "Namespace onde o PostgreSQL foi instalado"
  value       = kubernetes_namespace.postgres.metadata[0].name
}

output "internal_host" {
  description = "Host interno do PostgreSQL (DNS do Service) para uso no Meltano/DAGs"
  value = "${kubernetes_service_v1.postgres.metadata[0].name}.${kubernetes_namespace.postgres.metadata[0].name}.svc.cluster.local"
}

output "port" {
  description = "Porta do PostgreSQL"
  value       = "5432"
}

output "database" {
  description = "Database de destino criado no PostgreSQL"
  value       = var.postgres_db
}

output "service_name" {
  description = "Nome do Service do PostgreSQL (para port-forward)"
  value       = kubernetes_service_v1.postgres.metadata[0].name
}
