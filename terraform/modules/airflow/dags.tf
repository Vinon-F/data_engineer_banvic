# Empacota os arquivos de dags/ num ConfigMap, montado no scheduler/dagProcessor/apiServer (ver main.tf) 
# cabe código de DAG, não dados (limite de 1MiB).

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

  # Monta cada DAG do ConfigMap como arquivo individual em /opt/airflow/dags, em vez da pasta inteira — o que evita o "recursive loop" na descoberta de DAGs do Airflow.
  dags_volume_mounts = [
    for f in local.dag_files : {
      name      = "dags"
      mountPath = "/opt/airflow/dags/${f}"
      subPath   = f
    }
  ]

  # Hash do conteúdo das DAGs: ao mudar, força os pods a reiniciarem no `terraform apply` para pegar a nova versão.
  dags_checksum = sha256(jsonencode(kubernetes_config_map.dags.data))
}
