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

### 6. Rodar o pipeline

### 7. Conferir os dados
