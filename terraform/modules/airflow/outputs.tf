output "namespace" {
  description = "Namespace onde o Airflow foi instalado"
  value       = kubernetes_namespace.airflow.metadata[0].name
}
