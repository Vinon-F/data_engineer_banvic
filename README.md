<<<<<<< HEAD
## EM CONSTRUÇÃO ...
=======
# Pipeline de ingestão (Airflow + Meltano + PostgreSQL)

POC de infraestrutura e pipeline de dados: dump diário do ERP on-premises do
BanVic (um `.zip` de CSVs) é detectado pelo Airflow, extraído/carregado no
PostgreSQL pelo Meltano e validado em Kubernetes local (minikube), provisionado por Terraform.

## Arquitetura

## Pré-requisitos

## Passo a passo

### 1. Cluster

```bash
minikube start --driver=docker -p banvic
```

### 2. Imagem do Meltano

```bash
docker build -t banvic-meltano:v1.0 -f meltano/dockerfile meltano/
minikube image load banvic-meltano:v1.0 -p banvic
```

### 3. Drop zone on-premises simulada

```bash
minikube mount "$(git rev-parse --show-toplevel)/banvic_data:/mnt/on_premise_drop" -p banvic --uid 50000 --gid 0 &
```

```bash
cd terraform && terraform init && terraform apply
```

Sobe: namespace `postgres` (StatefulSet com a imagem oficial `postgres:16`, db
`banvic_dw`), namespace `airflow` (chart `apache-airflow/airflow`, LocalExecutor),
o Secret `meltano-postgres-credentials`, o PV/PVC `on-premise-drop` e o ConfigMap
das DAGs.

### 5. Acessar as UIs / serviços (port-forward)

Em terminais separados (ou com `&` ao final):

```bash
# PostgreSQL de destino (Data Warehouse simulado)
kubectl --context banvic -n postgres port-forward svc/postgres 5432:5432

# Airflow UI
kubectl --context banvic -n airflow port-forward svc/airflow-api-server 8080:8080
```

- PostgreSQL: `psql -h localhost -p 5432 -U banvic -d banvic_dw` (senha = var `postgres_password`).
- Airflow UI: http://localhost:8080 — login `admin` / var `airflow_admin_password`.

Para confirmar os nomes dos Services: `kubectl -n airflow get svc` e `kubectl -n postgres get svc`.

### 6. Rodar o pipeline

### 7. Conferir os dados
