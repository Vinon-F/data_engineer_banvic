# PostgreSQL de destino do pipeline (Data Warehouse simulado - camada "raw").
# StatefulSet de 1 réplica com a imagem oficial `postgres` e PVC dedicado, via
# manifests Kubernetes puros (sem Helm). Instância separada do metadata DB do Airflow.

resource "kubernetes_namespace" "postgres" {
  metadata {
    name = var.namespace
  }
}

resource "kubernetes_secret" "postgres" {
  metadata {
    name      = "postgres-credentials"
    namespace = kubernetes_namespace.postgres.metadata[0].name
  }

  data = {
    POSTGRES_USER     = var.postgres_user
    POSTGRES_PASSWORD = var.postgres_password
    POSTGRES_DB       = var.postgres_db
  }
}

resource "kubernetes_persistent_volume_claim_v1" "data" {
  metadata {
    name      = "postgres-data"
    namespace = kubernetes_namespace.postgres.metadata[0].name
  }

  spec {
    access_modes       = ["ReadWriteOnce"]
    storage_class_name = var.storage_class

    resources {
      requests = {
        storage = var.storage_size
      }
    }
  }

  # Não bloqueia o apply esperando o bind: o provisionador dinâmico só cria o PV
  # quando um Pod consome o PVC.
  wait_until_bound = false
}

resource "kubernetes_service_v1" "postgres" {
  metadata {
    name      = "postgres"
    namespace = kubernetes_namespace.postgres.metadata[0].name
  }

  spec {
    selector = {
      app = "postgres"
    }
    port {
      name        = "postgres"
      port        = 5432
      target_port = 5432
    }
    type = "ClusterIP"
  }
}

resource "kubernetes_stateful_set_v1" "postgres" {
  metadata {
    name      = "postgres"
    namespace = kubernetes_namespace.postgres.metadata[0].name
    labels = {
      app = "postgres"
    }
  }

  spec {
    service_name = kubernetes_service_v1.postgres.metadata[0].name
    replicas     = 1

    selector {
      match_labels = {
        app = "postgres"
      }
    }

    template {
      metadata {
        labels = {
          app = "postgres"
        }
      }

      spec {
        # A imagem oficial roda como uid 999; fs_group torna o volume gravável por esse usuário.
        security_context {
          fs_group = 999
        }

        container {
          name  = "postgres"
          image = var.image

          env_from {
            secret_ref {
              name = kubernetes_secret.postgres.metadata[0].name
            }
          }

          # Coloca os dados num subdiretório do volume, já que o initdb exige um diretório vazio e a raiz do mount ext4 vem com lost+found.
          env {
            name  = "PGDATA"
            value = "/var/lib/postgresql/data/pgdata"
          }

          port {
            name           = "postgres"
            container_port = 5432
          }

          volume_mount {
            name       = "data"
            mount_path = "/var/lib/postgresql/data"
          }

          resources {
            requests = {
              cpu    = "100m"
              memory = "256Mi"
            }
            limits = {
              cpu    = "500m"
              memory = "512Mi"
            }
          }

          readiness_probe {
            exec {
              command = ["sh", "-c", "pg_isready -U \"$POSTGRES_USER\" -d \"$POSTGRES_DB\""]
            }
            initial_delay_seconds = 10
            period_seconds        = 10
            timeout_seconds       = 5
            failure_threshold     = 6
          }

          liveness_probe {
            exec {
              command = ["sh", "-c", "pg_isready -U \"$POSTGRES_USER\" -d \"$POSTGRES_DB\""]
            }
            initial_delay_seconds = 30
            period_seconds        = 15
            timeout_seconds       = 5
            failure_threshold     = 6
          }
        }

        volume {
          name = "data"
          persistent_volume_claim {
            claim_name = kubernetes_persistent_volume_claim_v1.data.metadata[0].name
          }
        }
      }
    }
  }
}
