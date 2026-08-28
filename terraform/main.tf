module "minio" {
  source = "./modules/minio"

  root_user     = var.minio_root_user
  root_password = var.minio_root_password
}

module "airflow" {
  source = "./modules/airflow"

  admin_password = var.airflow_admin_password
  fernet_key     = var.airflow_fernet_key

  # Credenciais do MinIO repassadas como Secret para o Pod do Meltano
  # (KubernetesPodOperator, ver dags/banvic_meltano_dag.py) - esta sim é uma
  # dependência real: o endpoint interno só existe depois do MinIO instalado.
  minio_endpoint   = "http://${module.minio.internal_endpoint}"
  minio_access_key = var.minio_root_user
  minio_secret_key = var.minio_root_password

  depends_on = [module.minio]
}
