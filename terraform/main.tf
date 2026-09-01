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

  # Conexão com o PostgreSQL de destino, repassada como Secret para:
  #  - o Pod do Meltano (KubernetesPodOperator, target-postgres)
  #  - o scheduler (validate_task lê as contagens em raw.*)
  # Dependência real: o host interno só existe depois do PostgreSQL instalado.
  postgres_host     = module.postgres.internal_host
  postgres_port     = module.postgres.port
  postgres_user     = var.postgres_user
  postgres_password = var.postgres_password
  postgres_db       = var.postgres_db

  depends_on = [module.postgres]
}
