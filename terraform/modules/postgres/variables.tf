variable "postgres_user" {
  description = "Usuário da aplicação / dono do database de destino"
  type        = string
  default     = "banvic"
}

variable "postgres_password" {
  description = "Senha do usuário da aplicação no PostgreSQL"
  type        = string
  sensitive   = true
}

variable "postgres_db" {
  description = "Database de destino (Data Warehouse simulado)"
  type        = string
  default     = "banvic_dw"
}

variable "namespace" {
  description = "Namespace Kubernetes onde o PostgreSQL é instalado"
  type        = string
  default     = "postgres"
}

variable "image" {
  description = "Imagem do PostgreSQL (Docker Official Image)"
  type        = string
  default     = "postgres:16"
}

variable "storage_size" {
  description = "Tamanho do volume persistente do PostgreSQL"
  type        = string
  default     = "4Gi"
}

variable "storage_class" {
  description = "StorageClass usada pelo PVC do PostgreSQL (provisionamento dinâmico)"
  type        = string
  default     = "standard"
}
