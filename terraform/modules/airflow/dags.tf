# Publica o conteúdo de dags/ (raiz do repo) como ConfigMap, montado via
# extraVolumes/extraVolumeMounts no scheduler/dagProcessor/apiServer (ver
# main.tf). Atualizar uma DAG é só rodar `terraform apply` de novo - o
# kubelet sincroniza o volume sem precisar reiniciar os pods. ConfigMap tem
# limite de 1MiB total, suficiente para código de DAG (não para dados).
resource "kubernetes_config_map" "dags" {
  metadata {
    name      = "airflow-dags"
    namespace = kubernetes_namespace.airflow.metadata[0].name
  }

  data = {
    for f in fileset("${path.root}/../dags", "*.py") : f => file("${path.root}/../dags/${f}")
  }
}
