variable "admin_password" {
  description = "Senha do usuário admin da UI do Airflow"
  type        = string
  sensitive   = true
}

variable "fernet_key" {
  description = "Fernet key usada pelo Airflow para criptografar connections/variables"
  type        = string
  sensitive   = true
}

variable "namespace" {
  description = "Namespace Kubernetes onde o Airflow é instalado"
  type        = string
  default     = "airflow"
}

variable "admin_username" {
  description = "Usuário admin da UI do Airflow"
  type        = string
  default     = "admin"
}

variable "chart_version" {
  description = "Versão do chart apache-airflow/airflow"
  type        = string
  default     = "1.22.0"
}

# --- Credenciais do MinIO para o Pod do Meltano (KubernetesPodOperator) ---
variable "minio_endpoint" {
  description = "Endpoint interno do MinIO (S3-compatible), ex: http://minio.minio.svc.cluster.local:9000"
  type        = string
}

variable "minio_access_key" {
  description = "Access key do MinIO injetada no Pod do Meltano via Secret"
  type        = string
}

variable "minio_secret_key" {
  description = "Secret key do MinIO injetada no Pod do Meltano via Secret"
  type        = string
  sensitive   = true
}
