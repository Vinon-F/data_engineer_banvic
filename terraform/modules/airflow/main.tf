resource "kubernetes_namespace" "airflow" {
  metadata {
    name = var.namespace
  }
}

locals {
  # Volume/mount do ConfigMap com as DAGs (kubernetes_config_map.dags, em
  # dags.tf), montado nos 3 componentes que precisam enxergar dags/:
  # dagProcessor (parseia), apiServer (aba "Code" da UI) e scheduler (o
  # LocalExecutor roda as tasks no próprio pod do scheduler).
  dags_volume = {
    name = "dags"
    configMap = {
      name = kubernetes_config_map.dags.metadata[0].name
    }
  }
  dags_volume_mount = {
    name      = "dags"
    mountPath = "/opt/airflow/dags"
  }
}

resource "helm_release" "airflow" {
  name             = "airflow"
  repository       = "https://airflow.apache.org"
  chart            = "airflow"
  version          = var.chart_version
  namespace        = kubernetes_namespace.airflow.metadata[0].name
  create_namespace = false
  depends_on       = [kubernetes_namespace.airflow]

  # Timeout maior que o padrão (300s): a imagem apache/airflow:3.2.2 tem ~2.2Gi
  # e é baixada por 3 pods (scheduler, api-server, dag-processor), o que sozinho
  # já pode consumir boa parte de 5 minutos numa primeira instalação.
  timeout = 900

  # migrateDatabaseJob/createUserJob rodam como Jobs normais (useHelmHooks
  # false, ver abaixo), então precisamos que o Terraform espere eles
  # completarem, não só os Deployments/StatefulSets ficarem "Ready".
  wait_for_jobs = true

  values = [
    yamlencode({
      executor = "LocalExecutor"

      # CeleryExecutor-only components - desligados para caber nos recursos
      # padrão do minikube; não são necessários com LocalExecutor.
      redis = {
        enabled = false
      }
      flower = {
        enabled = false
      }
      statsd = {
        enabled = false
      }
      # Só necessário para deferrable operators/sensors assíncronos.
      # Revisitar quando a etapa de sensors (desafio, etapa 3) entrar.
      triggerer = {
        enabled = false
      }

      # Por padrão o chart roda migrateDatabaseJob/createUserJob como Helm
      # hooks (post-install). O provider Terraform do Helm não dispara esses
      # hooks como a CLI faz, então o Job nunca é criado e os pods ficam
      # presos esperando uma migration que nunca roda. A doc do chart recomenda
      # useHelmHooks=false para instalação via Terraform/ArgoCD/Flux.
      migrateDatabaseJob = {
        useHelmHooks   = false
        applyCustomEnv = false
      }

      # Airflow 3.x: o antigo componente "webserver" virou "apiServer", e o
      # usuário admin default é criado por um Job próprio (createUserJob),
      # não mais por webserver.defaultUser.
      createUserJob = {
        useHelmHooks   = false
        applyCustomEnv = false
        defaultUser = {
          enabled  = true
          username = var.admin_username
          role     = "Admin"
          email    = "admin@example.com"
        }
      }

      apiServer = {
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
        extraVolumes      = [local.dags_volume]
        extraVolumeMounts = [local.dags_volume_mount]
      }

      dagProcessor = {
        resources = {
          requests = {
            cpu    = "100m"
            memory = "256Mi"
          }
          limits = {
            cpu    = "500m"
            memory = "512Mi"
          }
        }
        extraVolumes      = [local.dags_volume]
        extraVolumeMounts = [local.dags_volume_mount]
      }

      scheduler = {
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
        extraVolumes      = [local.dags_volume]
        extraVolumeMounts = [local.dags_volume_mount]
      }

      postgresql = {
        enabled = true
        primary = {
          persistence = {
            size = "4Gi"
          }
          resources = {
            requests = {
              cpu    = "100m"
              memory = "256Mi"
            }
            limits = {
              cpu    = "500m"
              memory = "512Mi"
            }
          }
        }
      }
    })
  ]

  set_sensitive {
    name  = "createUserJob.defaultUser.password"
    value = var.admin_password
  }

  set_sensitive {
    name  = "fernetKey"
    value = var.fernet_key
  }
}
