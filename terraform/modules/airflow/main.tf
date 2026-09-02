resource "kubernetes_namespace" "airflow" {
  metadata {
    name = var.namespace
  }
}

locals {
  # Volume do ConfigMap com as DAGs (kubernetes_config_map.dags e
  # local.dags_volume_mounts/dags_checksum, ambos em dags.tf), montado nos 3
  # componentes que precisam enxergar dags/: dagProcessor (parseia),
  # apiServer (aba "Code" da UI) e scheduler (o LocalExecutor roda as tasks
  # no próprio pod do scheduler).
  dags_volume = {
    name = "dags"
    configMap = {
      name = kubernetes_config_map.dags.metadata[0].name
    }
  }

  # Drop zone on-premises simulada (PV/PVC em drop_zone.tf). Montada nos mesmos
  # 3 componentes que enxergam as DAGs: o scheduler roda unzip_task/validate_task/
  # cleanup_task (LocalExecutor) e o FileSensor precisa ver o .zip; dagProcessor
  # e apiServer entram só por consistência de spec.
  drop_zone_volume = {
    name = "on-premise-drop"
    persistentVolumeClaim = {
      claimName = kubernetes_persistent_volume_claim_v1.on_premise_drop.metadata[0].name
    }
  }
  drop_zone_volume_mount = {
    name      = "on-premise-drop"
    mountPath = var.drop_zone_mount_path
    # hostPath usa mountPropagation None por padrao: o `minikube mount` (9p) so
    # aparece dentro do pod se ja estava montado no no antes do pod subir.
    # HostToContainer (rslave) faz o mount do host propagar pra dentro do
    # container independente da ordem, sem precisar recriar o pod.
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

      # Conexão `fs_default` que o FileSensor (wait_for_legacy_zip) usa. O Airflow
      # já cria essa conexão por padrão no `db migrate`, mas declarar aqui deixa
      # explícita a dependência e a torna imune a LOAD_DEFAULT_CONNECTIONS=False.
      # `fs://` = conn_type fs sem basepath -> o `filepath` da DAG é absoluto.
      env = [
        {
          name  = "AIRFLOW_CONN_FS_DEFAULT"
          value = "fs://"
        },
      ]

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
        # validate_task (PythonOperator, roda no scheduler com LocalExecutor) le
        # POSTGRES_HOST/PORT/USER/PASSWORD/DB do ambiente. Sem credencial na DAG.
        # Usa scheduler.env (não extraEnvFrom): o schema do chart só aceita
        # extraEnvFrom no nível global (afeta todos os componentes), e
        # scheduler tem additionalProperties=false.
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
