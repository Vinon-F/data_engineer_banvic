# BanVic — Pipeline de ingestão ELT (Airflow + Meltano + PostgreSQL)

POC de infraestrutura e pipeline de dados: o dump diário do ERP on-premises do
BanVic (um `.zip` de CSVs) é detectado pelo Airflow, extraído/carregado no
PostgreSQL (camada `raw`) pelo Meltano e validado — tudo em Kubernetes local
(minikube), provisionado por Terraform.

## Arquitetura

```
Host (WSL)                              minikube (cluster "banvic")
┌───────────────────────┐               ┌────────────────────────────────────────────┐
│ banvic_data/           │  minikube     │ namespace: airflow                          │
│  banvic_data_<data>.zip │  mount (9p)  │  scheduler / dagProcessor / apiServer       │
└───────────────────────┘ ─────────────▶│   ├─ volume "dags"  (ConfigMap)             │
                       /mnt/on_premise_  │   └─ volume "on-premise-drop" (PVC→hostPath)│
                            drop         │        /opt/airflow/data/on_premise_drop    │
                                         │                                            │
                                         │  DAG banvic_meltano_extract_load            │
                                         │   FileSensor → unzip → run_meltano →        │
                                         │   validate_load → cleanup                   │
                                         │        │ KubernetesPodOperator              │
                                         │        ▼                                    │
                                         │  Pod efêmero (imagem banvic-meltano)        │
                                         │   meltano run tap-csv target-postgres       │
                                         │        │                                    │
                                         │        ▼                                    │
                                         │ namespace: postgres                         │
                                         │   svc/postgres  (db banvic_dw,              │
                                         │                  schema raw)                │
                                         └────────────────────────────────────────────┘
```

## Pré-requisitos

- minikube (driver docker), Docker, Terraform, `kubectl` (ou o embutido: `minikube kubectl -p banvic --`)
- `secrets.auto.tfvars` em `terraform/` (copie de `terraform/secrets.auto.tfvars.example`):

  ```hcl
  postgres_password      = "..."
  airflow_admin_password = "..."
  airflow_fernet_key     = "..."   # python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
  ```

## Passo a passo

### 1. Cluster

```bash
minikube start --driver=docker -p banvic
```

### 2. Imagem do Meltano

Sempre que algo em `meltano/` mudar:

```bash
docker build -t banvic-meltano:v1.0 -f meltano/dockerfile meltano/
minikube image load banvic-meltano:v1.0 -p banvic
```

### 3. Drop zone on-premises simulada

Deixe rodando em background enquanto for disparar a DAG. **O `--uid 50000` é
obrigatório**: o scheduler do Airflow roda como o usuário `airflow` (UID 50000) e
precisa escrever/apagar nessa pasta (tasks `unzip_zip` / `cleanup`).

```bash
minikube mount "$(git rev-parse --show-toplevel)/banvic_data:/mnt/on_premise_drop" -p banvic --uid 50000 --gid 0 &
```

### 4. Infraestrutura

```bash
cd terraform && terraform init && terraform apply
```

Sobe: namespace `postgres` (StatefulSet com a imagem oficial `postgres:16`, db
`banvic_dw`), namespace `airflow` (chart `apache-airflow/airflow`, LocalExecutor),
o Secret `meltano-postgres-credentials`, o PV/PVC `on-premise-drop` e o ConfigMap
das DAGs.

### 5. Acessar as UIs / serviços (port-forward)

```bash
kubectl port-forward -n airflow  svc/airflow-api-server 8080:8080 &
kubectl port-forward -n postgres svc/postgres 5432:5432 &
wait
```

- Airflow UI: http://localhost:8080 — usuário `admin`, senha `airflow_admin_password`
- PostgreSQL: `psql "postgresql://banvic:<postgres_password>@localhost:5432/banvic_dw"`

### 6. Rodar o pipeline

1. Coloque o dump do dia na drop zone, nomeado pela **data lógica** da execução:

   ```bash
   cp "banvic_data/Dados Banvic.zip" "banvic_data/banvic_data_$(date +%F).zip"
   ```

2. Na UI do Airflow, dispare a DAG `banvic_meltano_extract_load` com logical date =
   a mesma data do arquivo. Ordem das tasks:

   | Task | O que faz |
   |------|-----------|
   | `wait_for_legacy_zip` | `FileSensor` (reschedule) aguardando `banvic_data_<data>.zip` |
   | `unzip_zip` | descompacta os CSVs em `.../on_premise_drop/csvs/` |
   | `run_meltano_tap_csv_target_postgres` | Pod efêmero: `meltano run tap-csv target-postgres` (upsert em `raw.*`) |
   | `validate_load` | checa invariantes de integridade em `raw.*` |
   | `cleanup` | apaga o `.zip` e os CSVs processados (arquivamento simulado) |

### 7. Conferir os dados

```bash
psql "postgresql://banvic:<senha>@localhost:5432/banvic_dw" -c "\dt raw.*" -c "select count(*) from raw.transacoes;"
```

Checagem externa independente (com `meltano/.env` apontando para `localhost:5432` e
os CSVs em `meltano/data/csvs/`):

```bash
cd meltano && python validate.py
```

## Estratégia de ingestão

- **Extração/carga**: Meltano, `tap-csv` (schema das 7 entidades em
  `meltano/files_def.json`) → `target-postgres` (variant `meltanolabs`).
- **Incremental por MERGE**: `load_method: upsert` usando a PK de cada entidade —
  PK nova = INSERT, PK existente = UPDATE. Re-rodar o mesmo arquivo é no-op
  (idempotente); um arquivo novo a cada dia acumula o estado atual no DW.
- **Detecção do arquivo**: `FileSensor` em `mode="reschedule"` (libera o worker
  entre os pokes) sobre a convenção `banvic_data_{{ ds }}.zip`.
- **Resiliência**: `retries=2` / `retry_delay=5min` nas tasks de trabalho;
  `max_active_runs=1`; `cleanup` só roda em `all_success` (falha preserva o
  material para debug).
- **Segredos**: credenciais do Postgres só existem no Secret
  `meltano-postgres-credentials` (Terraform) — injetado no Pod do Meltano
  (`env_from`) e no scheduler (`extraEnvFrom`). Nada hardcoded nas DAGs.
