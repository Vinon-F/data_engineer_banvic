## EM CONSTRUÇÃO ...

```bash
minikube start --driver=docker -p banvic
```

```bash
kubectl config current-context
```

## Acessar as UIs (port-forward)

Depois do `terraform apply`, em dois terminais separados (o `kubectl port-forward` fica ocupando o terminal):

```bash
kubectl port-forward -n minio svc/minio-console 9001:9001
```
```bash
kubectl port-forward -n airflow svc/airflow-api-server 8080:8080
```

Ou os dois de uma vez, num único terminal (roda em background; `Ctrl+C` encerra ambos):
```bash
kubectl port-forward -n minio svc/minio-console 9001:9001 & kubectl port-forward -n airflow svc/airflow-api-server 8080:8080 & wait
```

- MinIO console: http://localhost:9001 (usuário/senha = `minio_root_user`/`minio_root_password` do `secrets.auto.tfvars`)
- Airflow UI: http://localhost:8080 (usuário/senha = `admin`/`airflow_admin_password` do `secrets.auto.tfvars`)
