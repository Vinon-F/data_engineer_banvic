# Mailpit: servidor SMTP "fake" + UI web. Captura os e-mails de notificação da DAG
# (falha via `email_on_failure` e sucesso via a task `notify_success`) sem precisar
# de conta de e-mail nem chave de API — nada sai do cluster.
#
# O Airflow aponta a connection `smtp_default` para o Service abaixo
# (ver `AIRFLOW_CONN_SMTP_DEFAULT` em main.tf). Para ver os e-mails:
#   kubectl --context banvic -n airflow port-forward svc/mailpit 8025:8025
#   -> http://localhost:8025

resource "kubernetes_deployment_v1" "mailpit" {
  metadata {
    name      = "mailpit"
    namespace = kubernetes_namespace.airflow.metadata[0].name
    labels = {
      app = "mailpit"
    }
  }

  spec {
    replicas = 1

    selector {
      match_labels = {
        app = "mailpit"
      }
    }

    template {
      metadata {
        labels = {
          app = "mailpit"
        }
      }

      spec {
        container {
          name              = "mailpit"
          image             = var.mailpit_image
          image_pull_policy = "IfNotPresent"

          port {
            name           = "smtp"
            container_port = 1025
          }
          port {
            name           = "http"
            container_port = 8025
          }

          resources {
            requests = {
              cpu    = "25m"
              memory = "64Mi"
            }
            limits = {
              cpu    = "100m"
              memory = "128Mi"
            }
          }

          readiness_probe {
            http_get {
              path = "/"
              port = "http"
            }
            initial_delay_seconds = 5
            period_seconds        = 10
          }

          liveness_probe {
            http_get {
              path = "/"
              port = "http"
            }
            initial_delay_seconds = 10
            period_seconds        = 20
          }
        }
      }
    }
  }
}

resource "kubernetes_service_v1" "mailpit" {
  metadata {
    name      = "mailpit"
    namespace = kubernetes_namespace.airflow.metadata[0].name
    labels = {
      app = "mailpit"
    }
  }

  spec {
    type = "ClusterIP"

    selector = {
      app = "mailpit"
    }

    port {
      name        = "smtp"
      port        = 1025
      target_port = "smtp"
    }
    port {
      name        = "http"
      port        = 8025
      target_port = "http"
    }
  }
}
