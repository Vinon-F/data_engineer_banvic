"""Ingestão ELT do ERP on-premises (BanVic) orquestrada pelo Airflow.

Fluxo (simula um funcionário do BanVic depositando o dump diário do ERP num
servidor de arquivos on-premises):

  wait_for_legacy_zip  FileSensor  -> {DROP}/banvic_data_{{ ds }}.zip
        |                              (mode=reschedule, poke 30s, timeout 10min)
        v
  unzip_zip            descompacta o .zip do dia em {DROP}/csvs/*.csv
        v
  run_meltano          KubernetesPodOperator -> `meltano run tap-csv target-postgres`
        |                   (Pod efêmero, imagem banvic-meltano; carrega raw.* via upsert)
        v
  validate_load        checa invariantes de integridade em raw.* (PythonOperator)
        v
  cleanup              apaga o .zip e os CSVs processados (arquivamento simulado)

A drop zone é o PVC `on-premise-drop` (Terraform: terraform/modules/airflow/
drop_zone.tf), um hostPath do nó do minikube alimentado por
`minikube mount "<repo>/banvic_data:/mnt/on_premise_drop" -p banvic --uid 50000 --gid 0`
(o `--uid 50000` é obrigatório: o scheduler roda como o usuário `airflow` (UID
50000) e precisa escrever/apagar na pasta em unzip_zip/cleanup).
"""

from __future__ import annotations

import logging
import os
import shutil
import zipfile
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.sensors.filesystem import FileSensor
from kubernetes.client import models as k8s

from banvic_validate import pg_params_from_env, validate

logger = logging.getLogger(__name__)

# Montado no scheduler/dagProcessor/apiServer via extraVolumes do chart do Airflow
# (terraform/modules/airflow/main.tf), a partir do PVC `on-premise-drop`.
DROP_ZONE = "/opt/airflow/data/on_premise_drop"
CSV_DIR = f"{DROP_ZONE}/csvs"
DROP_ZONE_PVC = "on-premise-drop"

# Convenção do "sistema legado": um dump por dia, nomeado pela data.
ZIP_TEMPLATE = f"{DROP_ZONE}/banvic_data_{{{{ ds }}}}.zip"

MELTANO_IMAGE = "banvic-meltano:v1.0"
PG_SECRET = "meltano-postgres-credentials"


def _unzip(ds: str) -> None:
    """Extrai os .csv do dump do dia em CSV_DIR, achatando subpastas."""
    zip_path = f"{DROP_ZONE}/banvic_data_{ds}.zip"

    # Começa de uma pasta limpa para um re-run não misturar arquivos antigos.
    if os.path.isdir(CSV_DIR):
        shutil.rmtree(CSV_DIR)
    os.makedirs(CSV_DIR, exist_ok=True)

    with zipfile.ZipFile(zip_path) as zf:
        members = [m for m in zf.namelist() if m.lower().endswith(".csv")]
        if not members:
            raise RuntimeError(f"{zip_path} não contém nenhum .csv")
        for member in members:
            target = os.path.join(CSV_DIR, os.path.basename(member))
            with zf.open(member) as src, open(target, "wb") as dst:
                shutil.copyfileobj(src, dst)
            logger.info("extraído %s -> %s", member, target)


def _validate() -> None:
    validate(csv_dir=CSV_DIR, pg=pg_params_from_env())


def _cleanup(ds: str) -> None:
    """Arquivamento simulado: remove o dump e os CSVs já carregados no DW."""
    zip_path = f"{DROP_ZONE}/banvic_data_{ds}.zip"
    if os.path.exists(zip_path):
        os.remove(zip_path)
        logger.info("removido %s", zip_path)
    if os.path.isdir(CSV_DIR):
        shutil.rmtree(CSV_DIR)
        logger.info("removido %s", CSV_DIR)


default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="banvic_meltano_extract_load",
    description=(
        "Detecta o dump diário do ERP on-premises (BanVic) via FileSensor, "
        "extrai/carrega no PostgreSQL (camada raw) via Meltano e valida a carga."
    ),
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["banvic", "meltano", "elt"],
) as dag:
    wait_for_legacy_zip = FileSensor(
        task_id="wait_for_legacy_zip",
        fs_conn_id="fs_default",
        filepath=ZIP_TEMPLATE,
        poke_interval=30,
        timeout=60 * 10,
        mode="reschedule",
        retries=0,
    )

    unzip_zip = PythonOperator(
        task_id="unzip_zip",
        python_callable=_unzip,
        op_kwargs={"ds": "{{ ds }}"},
    )

    run_meltano = KubernetesPodOperator(
        task_id="run_meltano_tap_csv_target_postgres",
        name="banvic-meltano",
        namespace="airflow",
        image=MELTANO_IMAGE,
        image_pull_policy="IfNotPresent",
        # ENTRYPOINT da imagem é `meltano`; só passamos os argumentos.
        arguments=["run", "tap-csv", "target-postgres"],
        volumes=[
            k8s.V1Volume(
                name=DROP_ZONE_PVC,
                persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(
                    claim_name=DROP_ZONE_PVC
                ),
            )
        ],
        volume_mounts=[
            k8s.V1VolumeMount(
                name=DROP_ZONE_PVC,
                mount_path="/project/data/csvs",
                sub_path="csvs",
                read_only=True,
            )
        ],
        env_from=[
            k8s.V1EnvFromSource(
                secret_ref=k8s.V1SecretEnvSource(name=PG_SECRET)
            )
        ],
        is_delete_operator_pod=True,
        get_logs=True,
        in_cluster=True,
    )

    validate_load = PythonOperator(
        task_id="validate_load",
        python_callable=_validate,
    )

    cleanup = PythonOperator(
        task_id="cleanup",
        python_callable=_cleanup,
        op_kwargs={"ds": "{{ ds }}"},
        trigger_rule="all_success",
    )

    wait_for_legacy_zip >> unzip_zip >> run_meltano >> validate_load >> cleanup
