"""Ingestão EL do dump diário do ERP on-premises (BanVic) orquestrada pelo Airflow.

Só Extract + Load: os dados vão para `raw.*` do jeito que vierem. Validação e
transformação ficam para uma etapa dbt posterior.

Fluxo:

  wait_for_legacy_zip    FileSensor espera {DROP}/banvic_data_{{ ds }}.zip
        v
  claim_zip              move o dump -> {DROP}/archive/banvic_data_{{ ds }}.zip
        |                     (o .zip fica retido em archive/)
        v
  unzip_and_check        extrai os CSVs do dia em {DROP}/archive/{{ ds }}_csvs/
        |                     e exige as 7 entidades
        v
  run_meltano            KubernetesPodOperator -> `meltano run tap-csv target-postgres`
        |                     (pod efêmero; a pasta do dia é montada em
        |                     /project/data/csvs via subPathExpr; o tap-csv usa o
        |                     files_def.json da imagem; carrega raw.* via upsert)
        v
  delete_extracted_csvs  apaga {DROP}/archive/{{ ds }}_csvs/ ; o .zip permanece

Drop zone: PVC `on-premise-drop` (terraform/modules/airflow/drop_zone.tf), um
hostPath do minikube alimentado por `minikube mount`. O scheduler roda como UID
50000 e precisa de escrita na pasta.

Agendamento `@daily`: o dump na drop zone deve ser nomeado pela data lógica UTC
(`{{ ds }}`). `catchup=False`.
"""

from __future__ import annotations

import logging
import os
import shutil
import zipfile
from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.sensors.filesystem import FileSensor
from kubernetes.client import models as k8s

from banvic_validate import ENTITIES

logger = logging.getLogger(__name__)

# Caminho do PVC `on-premise-drop` montado nos pods do Airflow.
DROP_ZONE = "/opt/airflow/data/on_premise_drop"
# Dumps já ingeridos ficam retidos aqui.
ARCHIVE_DIR = f"{DROP_ZONE}/archive"
DROP_ZONE_PVC = "on-premise-drop"

# Convenção do "sistema legado": um dump por dia, nomeado pela data.
ZIP_TEMPLATE = f"{DROP_ZONE}/banvic_data_{{{{ ds }}}}.zip"

MELTANO_IMAGE = "banvic-meltano:v1.0"
PG_SECRET = "meltano-postgres-credentials"


def _day_dir(ds: str) -> str:
    """Pasta dos CSVs extraídos do dia; montada no pod em /project/data/csvs."""
    return f"{ARCHIVE_DIR}/{ds}_csvs"


def _claim_zip(ds: str) -> None:
    """Move o dump do dia da raiz da drop zone para archive/ (retenção permanente)."""
    src = f"{DROP_ZONE}/banvic_data_{ds}.zip"
    dst = f"{ARCHIVE_DIR}/banvic_data_{ds}.zip"
    os.makedirs(ARCHIVE_DIR, exist_ok=True)

    if not os.path.exists(src):
        # Re-run de um dia já processado: zip já está em archive/.
        if os.path.exists(dst):
            logger.info("dump de %s já arquivado em %s, seguindo", ds, dst)
            return
        raise AirflowException(f"nenhum dump para {ds}: nem {src} nem {dst}")

    shutil.move(src, dst)
    logger.info("movido %s -> %s", src, dst)


def _unzip(ds: str) -> None:
    """Extrai os .csv do dump do dia em _day_dir(ds) e exige as 7 entidades (ENTITIES).

    O tap-csv usa o files_def.json embutido na imagem (só schema: entity + keys);
    run_meltano monta esta pasta em /project/data/csvs.
    """
    zip_path = f"{ARCHIVE_DIR}/banvic_data_{ds}.zip"
    out_dir = _day_dir(ds)

    # Pasta limpa: um re-run não mistura arquivos antigos.
    if os.path.isdir(out_dir):
        shutil.rmtree(out_dir)
    os.makedirs(out_dir, exist_ok=True)

    with zipfile.ZipFile(zip_path) as zf:
        members = [m for m in zf.namelist() if m.lower().endswith(".csv")]
        if not members:
            raise AirflowException(f"{zip_path} não contém nenhum .csv")
        for member in members:
            target = os.path.join(out_dir, os.path.basename(member))
            with zf.open(member) as src, open(target, "wb") as dst:
                shutil.copyfileobj(src, dst)
            logger.info("extraído %s -> %s", member, target)

    extracted = {os.path.splitext(os.path.basename(m))[0] for m in members}
    missing = sorted(set(ENTITIES) - extracted)
    if missing:
        raise AirflowException(f"CSVs ausentes no dump {ds}: {missing}")


def _cleanup(ds: str) -> None:
    """Remove só os CSVs extraídos do dia; o .zip permanece em archive/."""
    out_dir = _day_dir(ds)
    if os.path.isdir(out_dir):
        shutil.rmtree(out_dir)
        logger.info("removido %s", out_dir)


default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="banvic_meltano_extract_load",
    description=(
        "Detecta o dump diário do ERP on-premises (BanVic) via FileSensor e "
        "extrai/carrega no PostgreSQL (camada raw) via Meltano."
    ),
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
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
        # Dia sem dump: run marcado como skipped em vez de failed.
        soft_fail=True,
        retries=0,
    )

    claim_zip = PythonOperator(
        task_id="claim_zip",
        python_callable=_claim_zip,
        op_kwargs={"ds": "{{ ds }}"},
    )

    unzip_and_check = PythonOperator(
        task_id="unzip_and_check",
        python_callable=_unzip,
        op_kwargs={"ds": "{{ ds }}"},
    )

    run_meltano = KubernetesPodOperator(
        task_id="run_meltano_tap_csv_target_postgres",
        name="banvic-meltano",
        namespace="airflow",
        image=MELTANO_IMAGE,
        image_pull_policy="IfNotPresent",
        # ENTRYPOINT da imagem é `meltano`; passamos só os argumentos.
        arguments=["run", "tap-csv", "target-postgres"],
        # Data passada ao subPathExpr do volume; o Kubernetes expande $(RUN_DS).
        env_vars={"RUN_DS": "{{ ds }}"},
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
                # Só a pasta do dia (archive/<ds>_csvs/) vira /project/data/csvs,
                # onde o files_def.json da imagem espera os arquivos.
                mount_path="/project/data/csvs",
                sub_path_expr="archive/$(RUN_DS)_csvs",
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

    delete_extracted_csvs = PythonOperator(
        task_id="delete_extracted_csvs",
        python_callable=_cleanup,
        op_kwargs={"ds": "{{ ds }}"},
        trigger_rule="all_success",
    )

    (
        wait_for_legacy_zip
        >> claim_zip
        >> unzip_and_check
        >> run_meltano
        >> delete_extracted_csvs
    )
