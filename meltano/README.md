# Meltano — BanVic EL

Extrai os CSVs do dump diário do ERP e carrega no PostgreSQL, schema `raw`,
via `upsert`. Só Extract + Load; transformação fica para o dbt.

```
tap-csv (meltanolabs)  ->  target-postgres (meltanolabs)  ->  raw.*
```

## Arquivos

| Arquivo            | Papel                                                             |
| ------------------ | ---------------------------------------------------------------- |
| `meltano.yml`      | Definição dos plugins e config do loader.                        |
| `files_def.json`   | Schema do `tap-csv`: entidade, caminho do CSV e chave primária. |
| `dockerfile`       | Imagem `banvic-meltano`; `ENTRYPOINT meltano`.                   |
| `requirements.txt` | `meltano` + `psycopg2-binary`.                                  |
| `.env`             | Credenciais do Postgres (não versionar valores reais).          |

## Entidades

`agencias`, `clientes`, `colaborador_agencia`, `colaboradores`, `contas`,
`propostas_credito`, `transacoes` — chaves em `files_def.json`. Os CSVs são
lidos de `data/csvs/*.csv`.

## Configuração

Loader lê estas variáveis de ambiente (ver `.env`):

```
POSTGRES_HOST  POSTGRES_PORT  POSTGRES_USER  POSTGRES_PASSWORD  POSTGRES_DB
```

Destino fixo: `default_target_schema: raw`, `load_method: upsert`.

## Rodar

Local:

```bash
meltano install
meltano run tap-csv target-postgres
```

Docker:

```bash
docker build -t banvic-meltano:v1.0 .
docker run --rm --env-file .env \
  -v /caminho/para/csvs:/project/data/csvs:ro \
  banvic-meltano:v1.0
```

Em produção quem dispara é a DAG `banvic_meltano_extract_load` (Airflow), que
monta a pasta de CSVs do dia em `/project/data/csvs` e roda o mesmo comando via
`KubernetesPodOperator`.
