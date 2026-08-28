from datetime import datetime

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s

# /mnt/banvic_data é o hostPath do nó do minikube, montado a partir da pasta
# banvic_data/ do host (ambiente "on-premises" simulado) via:
#   minikube start -p banvic --mount --mount-string="<repo>/banvic_data:/mnt/banvic_data"
BANVIC_DATA_VOLUME = k8s.V1Volume(
    name="banvic-data",
    host_path=k8s.V1HostPathVolumeSource(path="/mnt/banvic_data", type="Directory"),
)

# /data/banvic_data é o BANVIC_DATA_DIR default definido em meltano/dockerfile.
BANVIC_DATA_VOLUME_MOUNT = k8s.V1VolumeMount(
    name="banvic-data",
    mount_path="/data/banvic_data",
    read_only=True,
)

with DAG(
    dag_id="banvic_meltano_extract_load",
    description="Extrai o ERP on-premises (BanVic) via Meltano e carrega o parquet no MinIO (camada raw).",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["banvic", "meltano", "elt"],
) as dag:
    run_meltano = KubernetesPodOperator(
        task_id="run_meltano_tap_csv_target_s3",
        name="banvic-meltano",
        namespace="airflow",
        image="banvic-meltano:v1.0",
        image_pull_policy="IfNotPresent",
        # Usa o ENTRYPOINT/CMD default da imagem (descompacta o .zip e roda
        # `meltano run tap-csv target-s3`); explicitado aqui só para clareza.
        cmds=["/entrypoint.sh"],
        arguments=["run", "tap-csv", "target-s3"],
        volumes=[BANVIC_DATA_VOLUME],
        volume_mounts=[BANVIC_DATA_VOLUME_MOUNT],
        env_from=[
            k8s.V1EnvFromSource(
                secret_ref=k8s.V1SecretEnvSource(name="meltano-minio-credentials")
            )
        ],
        is_delete_operator_pod=True,
        get_logs=True,
        in_cluster=True,
    )
