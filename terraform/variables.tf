variable "kubeconfig_path" {
  description = "Caminho para o kubeconfig local"
  type        = string
  default     = "~/.kube/config"
}

variable "kube_context" {
  description = "Contexto do kubectl a ser usado (ex: banvic, criado por `minikube start --driver=docker -p banvic`)"
  type        = string
  default     = "banvic"
}

# --- MinIO ---
variable "minio_root_user" {
  description = "Usuário root do MinIO"
  type        = string
  default     = "minioadmin"
}

variable "minio_root_password" {
  description = "Senha root do MinIO"
  type        = string
  sensitive   = true
}

# --- Airflow ---
variable "airflow_admin_password" {
  description = "Senha do usuário admin da UI do Airflow"
  type        = string
  sensitive   = true
}

variable "airflow_fernet_key" {
  description = "Fernet key usada pelo Airflow para criptografar connections/variables"
  type        = string
  sensitive   = true
}
