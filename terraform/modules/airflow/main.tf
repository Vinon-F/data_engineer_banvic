resource "kubernetes_namespace" "airflow" {
  metadata {
    name = var.namespace
  }
}

locals {
  # Volume do ConfigMap com as DAGs (definido em dags.tf), montado no dagProcessor
  # (parseia), no apiServer (aba "Code" da UI) e no scheduler (roda as tasks via
  # LocalExecutor).
  dags_volume = {
    name = "dags"
    configMap = {
      name = kubernetes_config_map.dags.metadata[0].name
    }
  }

  # Drop zone on-premises simulada (PV/PVC em drop_zone.tf), montada nos mesmos 3
  # componentes: o scheduler roda as tasks e o FileSensor precisa enxergar o .zip.
  drop_zone_volume = {
    name = "on-premise-drop"
    persistentVolumeClaim = {
      claimName = kubernetes_persistent_volume_claim_v1.on_premise_drop.metadata[0].name
    }
  }
  drop_zone_volume_mount = {
    name      = "on-premise-drop"
    mountPath = var.drop_zone_mount_path
    # Propaga para o container os mounts feitos no nó depois do pod subir
    # (ex.: `minikube mount`), sem precisar recriar o pod.
    mountPropagation = "HostToContainer"
  }

  extra_volumes       = [local.dags_volume, local.drop_zone_volume]
  extra_volume_mounts = concat(local.dags_volume_mounts, [local.drop_zone_volume_mount])
}

resource "helm_release" "airflow" {
  name             = "airflow"
  repository       = "https://airflow.apache.org"
  chart            = "airflow"
  version          = var.chart_version
  namespace        = kubernetes_namespace.airflow.metadata[0].name
  create_namespace = false
  depends_on       = [kubernetes_namespace.airflow]

  # Acima do padrão (300s): a imagem apache/airflow:3.2.2 (~2.2Gi) é baixada por
  # 3 pods na primeira instalação.
  timeout = 900

  # Espera os Jobs de migração/criação de usuário completarem, não só os
  # Deployments/StatefulSets ficarem "Ready".
  wait_for_jobs = true

  values = [
    yamlencode({
      executor = "LocalExecutor"

      # Define a conexão `fs_default` usada pelo FileSensor. `fs://` = conn_type fs
      # sem basepath, então o `filepath` da DAG é absoluto.
      env = [
        {
          name  = "AIRFLOW_CONN_FS_DEFAULT"
          value = "fs://"
        },
      ]

      # Componentes exclusivos do CeleryExecutor: desnecessários com LocalExecutor.
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
      triggerer = {
        enabled = false
      }

      # Roda os Jobs de setup como Jobs normais em vez de Helm hooks post-install,
      # que o provider Terraform do Helm não dispara (recomendação da doc do chart
      # para Terraform/ArgoCD/Flux).
      migrateDatabaseJob = {
        useHelmHooks   = false
        applyCustomEnv = false
      }

      # Cria o usuário admin inicial (no Airflow 3.x isso é feito pelo createUserJob,
      # não mais por webserver.defaultUser).
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
        extraVolumes      = local.extra_volumes
        extraVolumeMounts = local.extra_volume_mounts
        podAnnotations = {
          "checksum/dags" = local.dags_checksum
        }
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
        extraVolumes      = local.extra_volumes
        extraVolumeMounts = local.extra_volume_mounts
        podAnnotations = {
          "checksum/dags" = local.dags_checksum
        }
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
        extraVolumes      = local.extra_volumes
        extraVolumeMounts = local.extra_volume_mounts
        # Injeta as credenciais do Postgres (do Secret) no pod do scheduler, onde
        # o LocalExecutor roda as tasks que leem POSTGRES_* do ambiente.
        env = [
          for key in ["POSTGRES_HOST", "POSTGRES_PORT", "POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_DB"] : {
            name = key
            valueFrom = {
              secretKeyRef = {
                name = kubernetes_secret.meltano_postgres.metadata[0].name
                key  = key
              }
            }
          }
        ]
        podAnnotations = {
          "checksum/dags" = local.dags_checksum
        }
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
