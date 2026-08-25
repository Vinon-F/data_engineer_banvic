module "minio" {
  source = "./modules/minio"

  root_user     = var.minio_root_user
  root_password = var.minio_root_password
}

module "airflow" {
  source = "./modules/airflow"

  admin_password = var.airflow_admin_password
  fernet_key     = var.airflow_fernet_key

  # Não é uma dependência real de infraestrutura (nenhum output do MinIO
  # alimenta o Airflow) - só ordena o apply para um log mais legível na demo.
  # A dependência real (DAGs lendo do MinIO) é em nível de aplicação/runtime.
  depends_on = [module.minio]
}
