# Conexão com o PostgreSQL de destino, consumida por:
#  - Pod do Meltano (KubernetesPodOperator, via env_from/secretRef) -> target-postgres
#  - scheduler do Airflow (via scheduler.extraEnvFrom em main.tf) -> validate_task
# Evita hardcodar credenciais nas DAGs.
resource "kubernetes_secret" "meltano_postgres" {
  metadata {
    name      = "meltano-postgres-credentials"
    namespace = kubernetes_namespace.airflow.metadata[0].name
  }

  data = {
    POSTGRES_HOST     = var.postgres_host
    POSTGRES_PORT     = var.postgres_port
    POSTGRES_USER     = var.postgres_user
    POSTGRES_PASSWORD = var.postgres_password
    POSTGRES_DB       = var.postgres_db
  }
}
