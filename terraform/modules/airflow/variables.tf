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

# --- Conexão com o PostgreSQL de destino ---
# Injetada como Secret no Pod do Meltano (KubernetesPodOperator) e no scheduler
variable "postgres_host" {
  description = "Host interno do PostgreSQL de destino (DNS do Service)"
  type        = string
}

variable "postgres_port" {
  description = "Porta do PostgreSQL de destino"
  type        = string
  default     = "5432"
}

variable "postgres_user" {
  description = "Usuário da aplicação no PostgreSQL de destino"
  type        = string
}

variable "postgres_password" {
  description = "Senha do usuário da aplicação no PostgreSQL de destino"
  type        = string
  sensitive   = true
}

variable "postgres_db" {
  description = "Database de destino (Data Warehouse simulado)"
  type        = string
}

# --- Drop zone on-premises simulada ---
variable "drop_zone_host_path" {
  description = "Caminho no nó do minikube (via `minikube mount`) usado como hostPath do PV da drop zone"
  type        = string
  default     = "/mnt/on_premise_drop"
}

variable "drop_zone_mount_path" {
  description = "Caminho onde a drop zone é montada dentro dos containers do Airflow"
  type        = string
  default     = "/opt/airflow/data/on_premise_drop"
}
