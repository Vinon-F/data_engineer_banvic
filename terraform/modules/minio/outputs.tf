output "namespace" {
  description = "Namespace onde o MinIO foi instalado"
  value       = kubernetes_namespace.minio.metadata[0].name
}

output "internal_endpoint" {
  description = "Endpoint interno do MinIO (S3-compatible) para uso no Meltano/DAGs"
  value       = "minio.${kubernetes_namespace.minio.metadata[0].name}.svc.cluster.local:9000"
}

output "buckets" {
  description = "Buckets criados no MinIO"
  value       = var.buckets
}
