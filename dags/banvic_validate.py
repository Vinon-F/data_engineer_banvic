"""Validação de integridade da carga BanVic no PostgreSQL (camada raw).

Usado pelo `validate_task` (PythonOperator) da DAG `banvic_meltano_extract_load`,
que roda no scheduler do Airflow — por isso mora em `dags/` (único diretório que
o scheduler enxerga) e não em `meltano/`. Também é reaproveitado pelo script
standalone `meltano/validate.py` para checagem local.

A carga é incremental (`load_method: upsert` no target-postgres), então o DW
acumula o estado de várias execuções e NÃO dá para exigir `count(pg) == count(csv)`.
A validação checa invariantes:

  1. Sem PK duplicada em `raw.<entidade>`  (o MERGE por chave manteve a unicidade)
  2. Toda PK do CSV do dia existe em `raw.<entidade>`  (a carga deste run entrou)
  3. `count(pg) >= nº de PKs distintas do CSV do dia`
"""

from __future__ import annotations

import csv
import logging
import os

logger = logging.getLogger(__name__)

# Entidades do tap-csv em files_conf.json
ENTITIES: dict[str, str] = {
    "agencias": "cod_agencia",
    "clientes": "cod_cliente",
    "colaborador_agencia": "cod_colaborador",
    "colaboradores": "cod_colaborador",
    "contas": "num_conta",
    "propostas_credito": "cod_proposta",
    "transacoes": "cod_transacao",
}

TARGET_SCHEMA = "raw"


def _csv_keys(path: str, key: str) -> list[str]:
    """Lê os valores da coluna-chave do CSV (como texto, respeitando aspas)."""
    with open(path, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        if reader.fieldnames is None or key not in reader.fieldnames:
            raise RuntimeError(
                f"{path}: coluna-chave '{key}' ausente (colunas: {reader.fieldnames})"
            )
        return [row[key] for row in reader]


def pg_params_from_env() -> dict[str, str]:
    """Lê a conexão do ambiente (Secret meltano-postgres-credentials)."""
    return {
        "host": os.environ["POSTGRES_HOST"],
        "port": os.environ.get("POSTGRES_PORT", "5432"),
        "user": os.environ["POSTGRES_USER"],
        "password": os.environ["POSTGRES_PASSWORD"],
        "dbname": os.environ["POSTGRES_DB"],
    }


def validate(csv_dir: str, pg: dict[str, str]) -> bool:
    """Checa as invariantes acima. Levanta RuntimeError se alguma falhar."""
    # Import adiado: psycopg2 não precisa existir no parse da DAG, só na execução.
    import psycopg2
    from psycopg2.extras import execute_values

    conn = psycopg2.connect(**pg)
    problemas: list[str] = []

    try:
        with conn.cursor() as cur:
            for entity, key in ENTITIES.items():
                csv_path = os.path.join(csv_dir, f"{entity}.csv")
                keys = _csv_keys(csv_path, key)
                n_csv, distinct_keys = len(keys), sorted(set(keys))

                cur.execute(
                    f'SELECT count(*), count(DISTINCT "{key}") '
                    f'FROM {TARGET_SCHEMA}."{entity}"'
                )
                n_pg, n_pg_distinct = cur.fetchone()
                n_dup = n_pg - n_pg_distinct

                cur.execute("CREATE TEMP TABLE _csv_keys (k text)")
                execute_values(
                    cur,
                    "INSERT INTO _csv_keys (k) VALUES %s",
                    [(k,) for k in distinct_keys],
                )
                cur.execute(
                    f'SELECT count(*) FROM _csv_keys c '
                    f'LEFT JOIN {TARGET_SCHEMA}."{entity}" t '
                    f'  ON t."{key}"::text = c.k '
                    f'WHERE t."{key}" IS NULL'
                )
                n_missing = cur.fetchone()[0]
                cur.execute("DROP TABLE _csv_keys")

                ok = (n_dup == 0) and (n_missing == 0) and (n_pg >= len(distinct_keys))
                if not ok:
                    problemas.append(
                        f"{entity}: pg={n_pg} pk_dup={n_dup} pk_faltando={n_missing}"
                    )
                logger.info(
                    "%-22s pg=%-8s csv=%-8s csv_pk_distintas=%-8s pk_dup=%-4s "
                    "pk_faltando=%-4s %s",
                    entity, n_pg, n_csv, len(distinct_keys), n_dup, n_missing,
                    "OK" if ok else "FALHOU",
                )
        conn.rollback()  # leitura + temp tables; nada a persistir
    finally:
        conn.close()

    if problemas:
        raise RuntimeError("Validação falhou para: " + "; ".join(problemas))

    logger.info("RESULTADO GERAL: OK (%d entidades)", len(ENTITIES))
    return True
