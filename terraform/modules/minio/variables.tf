variable "root_user" {
  description = "Usuário root do MinIO"
  type        = string
}

variable "root_password" {
  description = "Senha root do MinIO"
  type        = string
  sensitive   = true
}

variable "namespace" {
  description = "Namespace Kubernetes onde o MinIO é instalado"
  type        = string
  default     = "minio"
}

variable "storage_size" {
  description = "Tamanho do volume persistente do MinIO"
  type        = string
  default     = "10Gi"
}

variable "storage_class" {
  description = "StorageClass usada pelo PVC do MinIO"
  type        = string
  default     = "standard"
}

variable "chart_version" {
  description = "Versão do chart minio/minio"
  type        = string
  default     = "5.4.0"
}

variable "buckets" {
  description = "Buckets a criar no MinIO (camada raw do data lake)"
  type        = list(string)
  default     = ["raw"]
}
