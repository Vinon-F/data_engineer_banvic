# Credenciais do MinIO injetadas no Pod do Meltano (KubernetesPodOperator, via
# env_from/secretRef) - evita hardcodar credenciais na DAG.
resource "kubernetes_secret" "meltano_minio" {
  metadata {
    name      = "meltano-minio-credentials"
    namespace = kubernetes_namespace.airflow.metadata[0].name
  }

  data = {
    MINIO_ENDPOINT   = var.minio_endpoint
    MINIO_ACCESS_KEY = var.minio_access_key
    MINIO_SECRET_KEY = var.minio_secret_key
  }
}
