# PV/PVC (volume persistente) para simular conexão "SFPT" com servidor on premises no minikube.
#
# storage_class_name usa uma StorageClass explícita ("manual", sem provisioner) em vez
# de "" — o provider Terraform de Kubernetes omite o campo quando ele é uma string
# vazia, o que faz o admission controller de storage class padrão do cluster carimbar
# "standard" só no PVC (o PV não passa por esse admission controller), causando um
# VolumeMismatch que impede o bind.
resource "kubernetes_storage_class_v1" "manual" {
  metadata {
    name = "manual"
  }
  storage_provisioner = "kubernetes.io/no-provisioner"
}

resource "kubernetes_persistent_volume_v1" "on_premise_drop" {
  metadata {
    name = "on-premise-drop"
  }

  spec {
    capacity = {
      storage = "100Mi"
    }
    access_modes                     = ["ReadWriteOnce"]
    persistent_volume_reclaim_policy = "Retain"
    storage_class_name               = kubernetes_storage_class_v1.manual.metadata[0].name

    persistent_volume_source {
      host_path {
        path = var.drop_zone_host_path
        type = "DirectoryOrCreate"
      }
    }
  }
}

resource "kubernetes_persistent_volume_claim_v1" "on_premise_drop" {
  metadata {
    name      = "on-premise-drop"
    namespace = kubernetes_namespace.airflow.metadata[0].name
  }

  spec {
    access_modes       = ["ReadWriteOnce"]
    storage_class_name = kubernetes_storage_class_v1.manual.metadata[0].name
    volume_name        = kubernetes_persistent_volume_v1.on_premise_drop.metadata[0].name

    resources {
      requests = {
        storage = "100Mi"
      }
    }
  }

  wait_until_bound = true
}
