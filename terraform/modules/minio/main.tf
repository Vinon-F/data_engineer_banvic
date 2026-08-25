resource "kubernetes_namespace" "minio" {
  metadata {
    name = var.namespace
  }
}

resource "helm_release" "minio" {
  name             = "minio"
  repository       = "https://charts.min.io/"
  chart            = "minio"
  version          = var.chart_version
  namespace        = kubernetes_namespace.minio.metadata[0].name
  create_namespace = false
  depends_on       = [kubernetes_namespace.minio]

  values = [
    yamlencode({
      mode     = "standalone"
      replicas = 1

      persistence = {
        enabled      = true
        size         = var.storage_size
        storageClass = var.storage_class
      }

      resources = {
        requests = {
          cpu    = "250m"
          memory = "512Mi"
        }
        limits = {
          cpu    = "500m"
          memory = "1Gi"
        }
      }

      service = {
        type = "ClusterIP"
      }

      consoleService = {
        type = "ClusterIP"
      }
    })
  ]

  set_sensitive {
    name  = "rootUser"
    value = var.root_user
  }

  set_sensitive {
    name  = "rootPassword"
    value = var.root_password
  }
}

# Cria os buckets via um Job rodando o mc (MinIO Client) dentro do cluster,
# já que o MinIO só está exposto como ClusterIP (sem acesso da máquina local
# no momento do apply). ttl_seconds_after_finished autolimpa o Job depois de
# completar: se var.buckets mudar depois, o Job antigo já não existe mais e o
# Terraform recria sozinho (Job do Kubernetes é imutável depois de criado -
# sem a autolimpeza seria preciso apagar o Job manualmente antes do reapply,
# como tivemos que fazer com o createUserJob do Airflow).
resource "kubernetes_job_v1" "create_buckets" {
  metadata {
    name      = "minio-create-buckets"
    namespace = kubernetes_namespace.minio.metadata[0].name
  }

  spec {
    backoff_limit              = 3
    ttl_seconds_after_finished = 60

    template {
      metadata {
        name = "minio-create-buckets"
      }

      spec {
        restart_policy = "OnFailure"

        container {
          name    = "mc"
          image   = "minio/mc:latest"
          command = ["/bin/sh", "-c"]

          args = [
            <<-EOT
            set -eu
            mc alias set target http://minio.${kubernetes_namespace.minio.metadata[0].name}.svc.cluster.local:9000 "$MC_USER" "$MC_PASSWORD"
            %{ for bucket in var.buckets ~}
            mc mb --ignore-existing target/${bucket}
            %{ endfor ~}
            EOT
          ]

          env {
            name  = "MC_USER"
            value = var.root_user
          }

          env {
            name  = "MC_PASSWORD"
            value = var.root_password
          }
        }
      }
    }
  }

  wait_for_completion = true
  depends_on          = [helm_release.minio]
}
