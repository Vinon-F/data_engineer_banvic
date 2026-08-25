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
