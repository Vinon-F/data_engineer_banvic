"""Ingestão ELT do ERP on-premises (BanVic) orquestrada pelo Airflow.

Fluxo (simula um funcionário do BanVic depositando o dump diário do ERP num
servidor de arquivos on-premises):

  wait_for_legacy_zip  FileSensor  -> {DROP}/banvic_data_{{ ds }}.zip
        |                              (mode=reschedule, poke 30s, timeout 10min, soft_fail)
        v
  claim_zip            move o dump do dia -> {DROP}/archive/banvic_data_{{ ds }}.zip
        |                  (o .zip PERMANECE lá: retenção dos dumps já ingeridos)
        v
  unzip_and_check      extrai os CSVs em {DROP}/archive/banvic_data_{{ ds }}/ e exige as
        |                  7 entidades esperadas; gera o files_def.json (paths absolutos)
        |                  que o tap-csv lê dentro do pod do Meltano
        v
  run_meltano          KubernetesPodOperator -> `meltano run tap-csv target-postgres`
        |                   (Pod efêmero, imagem banvic-meltano; carrega raw.* via upsert)
        v
  validate_load        checa invariantes de integridade em raw.* (PythonOperator)
        v
  delete_extracted_csvs  apaga só {DROP}/archive/banvic_data_{{ ds }}/ ; o .zip permanece

A drop zone é o PVC `on-premise-drop` (Terraform: terraform/modules/airflow/
drop_zone.tf), um hostPath do nó do minikube alimentado por
`minikube mount "<repo>/banvic_data:/mnt/on_premise_drop" -p banvic --uid 50000 --gid 0`
(o `--uid 50000` é obrigatório: o scheduler roda como o usuário `airflow` (UID
50000) e precisa escrever/apagar na pasta em claim_zip/unzip_and_check/
delete_extracted_csvs).

Agendamento: `@daily`. O run da data lógica D dispara logo após D terminar e usa
`{{ ds }}` (data lógica em UTC) para localizar `banvic_data_{{ ds }}.zip`. O dump
deixado na drop zone PRECISA ser nomeado pela data lógica UTC; nomear pela data
local causa off-by-one na virada do dia. `catchup=False`: só o intervalo mais
recente é agendado automaticamente — dias perdidos se reprocessam re-disparando o
run com a data lógica desejada (o .zip fica retido em archive/).
"""

from __future__ import annotations

import json
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

from banvic_validate import ENTITIES, pg_params_from_env, validate

logger = logging.getLogger(__name__)

# Montado no scheduler/dagProcessor/apiServer via extraVolumes do chart do Airflow
# (terraform/modules/airflow/main.tf), a partir do PVC `on-premise-drop`.
DROP_ZONE = "/opt/airflow/data/on_premise_drop"
# Dumps já detectados são movidos para cá pela claim_zip e ficam retidos.
ARCHIVE_DIR = f"{DROP_ZONE}/archive"
DROP_ZONE_PVC = "on-premise-drop"

# Convenção do "sistema legado": um dump por dia, nomeado pela data.
ZIP_TEMPLATE = f"{DROP_ZONE}/banvic_data_{{{{ ds }}}}.zip"

MELTANO_IMAGE = "banvic-meltano:v1.0"
PG_SECRET = "meltano-postgres-credentials"

# `volume_mounts` do KubernetesPodOperator não é campo Jinja, então o mount aponta
# para o pai estático `archive/` e o caminho por-dia chega ao Meltano por este
# override de config (env_vars É renderizado). O tap-csv lê o manifesto gerado por
# unzip_and_check em vez do meltano/files_def.json commitado (esse fica só p/ runs locais).
MELTANO_FILES_DEF_IN_POD = "/project/data/archive/banvic_data_{{ ds }}/files_def.json"


def _day_dir(ds: str) -> str:
    """Pasta dos CSVs extraídos do dump do dia (mesmo nome do .zip, sem extensão)."""
    return f"{ARCHIVE_DIR}/banvic_data_{ds}"


def _claim_zip(ds: str) -> None:
    """Move o dump do dia da raiz da drop zone para archive/ (retenção permanente)."""
    src = f"{DROP_ZONE}/banvic_data_{ds}.zip"
    dst = f"{ARCHIVE_DIR}/banvic_data_{ds}.zip"
    os.makedirs(ARCHIVE_DIR, exist_ok=True)

    if not os.path.exists(src):
        # Re-run de um dia já processado: o zip já está em archive/. Segue sem erro.
        if os.path.exists(dst):
            logger.info("dump de %s já arquivado em %s, seguindo", ds, dst)
            return
        raise AirflowException(f"nenhum dump para {ds}: nem {src} nem {dst}")

    shutil.move(src, dst)
    logger.info("movido %s -> %s", src, dst)


def _unzip(ds: str) -> None:
    """Extrai os .csv do dump do dia em _day_dir(ds), achatando subpastas.

    Falha se faltar alguma das entidades esperadas (ENTITIES). Também grava o
    files_def.json que o tap-csv consome dentro do pod, com paths absolutos.
    """
    zip_path = f"{ARCHIVE_DIR}/banvic_data_{ds}.zip"
    out_dir = _day_dir(ds)

    # Começa de uma pasta limpa para um re-run não misturar arquivos antigos.
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

    files_def = [
        {
            "entity": entity,
            "path": f"/project/data/archive/banvic_data_{ds}/{entity}.csv",
            "keys": [key],
        }
        for entity, key in ENTITIES.items()
    ]
    manifest = os.path.join(out_dir, "files_def.json")
    with open(manifest, "w", encoding="utf-8") as fh:
        json.dump(files_def, fh, indent=2)
    logger.info("gerado %s (%d entidades)", manifest, len(files_def))


def _validate(ds: str) -> None:
    validate(csv_dir=_day_dir(ds), pg=pg_params_from_env())


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
        "Detecta o dump diário do ERP on-premises (BanVic) via FileSensor, "
        "extrai/carrega no PostgreSQL (camada raw) via Meltano e valida a carga."
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
        # Dia sem dump (ou dia já arquivado num re-run): marca o run como skipped
        # em vez de failed.
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
        # ENTRYPOINT da imagem é `meltano`; só passamos os argumentos.
        arguments=["run", "tap-csv", "target-postgres"],
        # Override do `csv_files_definition` do tap-csv: aponta para o manifesto
        # por-dia gerado em unzip_and_check. env_vars é renderizado por Jinja.
        env_vars={"TAP_CSV_CSV_FILES_DEFINITION": MELTANO_FILES_DEF_IN_POD},
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
                mount_path="/project/data/archive",
                sub_path="archive",
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
        op_kwargs={"ds": "{{ ds }}"},
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
        >> validate_load
        >> delete_extracted_csvs
    )
