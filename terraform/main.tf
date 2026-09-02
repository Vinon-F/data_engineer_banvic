module "postgres" {
  source = "./modules/postgres"

  postgres_user     = var.postgres_user
  postgres_password = var.postgres_password
  postgres_db       = var.postgres_db
}

module "airflow" {
  source = "./modules/airflow"

  admin_password = var.airflow_admin_password
  fernet_key     = var.airflow_fernet_key

  # PostgreSQL repassada como Secret para Airflow, KubernetesPodOperator, Meltano (target-postgres).
  postgres_host     = module.postgres.internal_host
  postgres_port     = module.postgres.port
  postgres_user     = var.postgres_user
  postgres_password = var.postgres_password
  postgres_db       = var.postgres_db

  depends_on = [module.postgres]
}
