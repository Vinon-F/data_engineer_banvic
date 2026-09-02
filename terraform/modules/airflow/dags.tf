# Publica o conteúdo de dags/ (raiz do repo) como ConfigMap, montado via
# extraVolumes/extraVolumeMounts no scheduler/dagProcessor/apiServer (ver
# main.tf). ConfigMap tem limite de 1MiB total, suficiente para código de
# DAG (não para dados).
resource "kubernetes_config_map" "dags" {
  metadata {
    name      = "airflow-dags"
    namespace = kubernetes_namespace.airflow.metadata[0].name
  }

  data = {
    for f in local.dag_files : f => file("${path.root}/../dags/${f}")
  }
}

locals {
  dag_files = fileset("${path.root}/../dags", "*.py")

  # Montar o ConfigMap inteiro como diretório (mountPath = "/opt/airflow/dags")
  # quebra o dag-processor: o kubelet usa um padrão "atomic writer" (arquivos
  # reais numa subpasta oculta com timestamp + symlink "..data" apontando pra
  # ela), e o walker de descoberta de DAGs do Airflow não ignora esses
  # entries ocultos - encontra o mesmo diretório real duas vezes e derruba o
  # container com "RuntimeError: Detected recursive loop when walking DAG
  # directory". Mount via subPath por arquivo evita essa estrutura de
  # symlinks (bind-mount direto do conteúdo, sem diretório oculto).
  dags_volume_mounts = [
    for f in local.dag_files : {
      name      = "dags"
      mountPath = "/opt/airflow/dags/${f}"
      subPath   = f
    }
  ]

  # subPath não recebe atualização automática do kubelet quando o ConfigMap
  # muda. Esse checksum vira podAnnotation em scheduler/apiServer/dagProcessor
  # (main.tf): mudar o conteúdo de uma DAG muda o hash, o que muda os values
  # do helm_release e força um rollout dos pods no próximo `terraform apply`.
  dags_checksum = sha256(jsonencode(kubernetes_config_map.dags.data))
}
