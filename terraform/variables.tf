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

# --- PostgreSQL (destino DW)
variable "postgres_user" {
  description = "Usuário da aplicação no PostgreSQL de destino"
  type        = string
  default     = "banvic"
}

variable "postgres_password" {
  description = "Senha do usuário da aplicação no PostgreSQL de destino"
  type        = string
  sensitive   = true
}

variable "postgres_db" {
  description = "Database de destino (Data Warehouse simulado)"
  type        = string
  default     = "banvic_dw"
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
