# Kubernetes

A mesma infra dos playbooks de `infra/playbooks/`, num cluster Kubernetes: RustFS
(lake), PostgreSQL + pg_duckdb (data warehouse) e Airflow. Tudo fica no namespace
`cidades`.

| Pasta | O que é | Como sobe |
| --- | --- | --- |
| `rustfs/` | RustFS 1.0.1, standalone | chart oficial `rustfs/rustfs` 1.0.1 |
| `postgres/` | PostgreSQL 15 + pg_duckdb 1.2.0 | operator CloudNativePG + imagem própria (`Dockerfile`) |
| `airflow/` | Airflow 3.3.2, LocalExecutor | chart oficial `apache-airflow/airflow` 1.22.0 + imagem de `infra/docker/airflow/` |
| `criar-secrets.sh` | todos os Secrets (senhas geradas no cluster) | `./criar-secrets.sh rustfs\|postgres\|airflow` |

Nenhuma senha fica no repositório. O `criar-secrets.sh` gera as senhas e as
grava só como Secrets do cluster. Ele é idempotente: rodar de novo não troca
nada.

## Pré-requisitos

- cluster com uma StorageClass padrão (no LAPPIS, `longhorn`; ajuste
  `storageclass.name` em `rustfs/values.yaml` e `storage` em
  `postgres/cluster.yaml`);
- `kubectl`, `helm` 3 e o `rc` (cliente do RustFS) na sua máquina;
- as duas imagens publicadas num registry que o cluster alcance:

```bash
# Postgres + pg_duckdb: compila o DuckDB, leva de 20 a 60 min
docker build -t ghcr.io/govhub-br/cidades-postgres:15-pgduckdb-ee38d3b infra/k8s/postgres
# Airflow: a mesma imagem do ambiente local
docker build -t ghcr.io/govhub-br/cidades-airflow:main -f infra/docker/airflow/Dockerfile .
docker push ghcr.io/govhub-br/cidades-postgres:15-pgduckdb-ee38d3b
docker push ghcr.io/govhub-br/cidades-airflow:main
```

```bash
helm repo add rustfs https://charts.rustfs.com
helm repo add cnpg https://cloudnative-pg.github.io/charts
helm repo add apache-airflow https://airflow.apache.org
helm repo update
```

## Ordem

### 1. RustFS

```bash
infra/k8s/criar-secrets.sh rustfs
helm upgrade --install rustfs rustfs/rustfs --version 1.0.1 \
  -n cidades -f infra/k8s/rustfs/values.yaml --wait
```

Bucket, policy e usuário de aplicação, pelo `rc` via port-forward. É a mesma
receita de `infra/playbooks/rustfs/policies/README.md`:

```bash
kubectl -n cidades port-forward svc/rustfs-svc 9000:9000 9001:9001 &
rc alias set cidades http://127.0.0.1:9000 rustfs-admin \
  "$(kubectl -n cidades get secret rustfs-admin -o jsonpath='{.data.RUSTFS_SECRET_KEY}' | base64 -d)"
rc mb cidades/data-lake-mcid
rc admin policy create cidades/ data-lake-mcid-readwrite data-lake-mcid-readwrite.json
openssl rand -hex 24 > app-cidades_secret_key
rc admin user add cidades/ app-cidades "$(cat app-cidades_secret_key)"
rc admin policy attach cidades/ data-lake-mcid-readwrite --user app-cidades
```

O `data-lake-mcid-readwrite.json` é a policy de exemplo do `policies/README.md`,
trocando `datalake` por `data-lake-mcid`.

### 2. Postgres + pg_duckdb

```bash
helm upgrade --install cnpg cnpg/cloudnative-pg --version 0.29.1 \
  -n cnpg-system --create-namespace --wait
infra/k8s/criar-secrets.sh postgres app-cidades_secret_key
rm app-cidades_secret_key          # agora ela vive no Secret cidades-lake
kubectl apply -f infra/k8s/postgres/cluster.yaml
kubectl -n cidades wait cluster/cidades-pg --for=condition=Ready --timeout=10m
kubectl apply -f infra/k8s/postgres/lake-secret.yaml
kubectl -n cidades wait job/cidades-pg-lake-secret --for=condition=Complete --timeout=5m
```

O `cluster.yaml` cria o banco `cidades` (DW) e o `airflow` (metadados), os
usuários `cidades`, `airflow` e `usuario_leitura` e o role `duckdb_users`. O Job
`lake-secret.yaml` liga o pg_duckdb ao RustFS (`SERVER lake_s3` + `USER MAPPING`
do `cidades`) e confere, como `cidades`, se o bucket é listável. Para trocar a
chave do lake, atualize o Secret `cidades-lake` e rode o Job de novo.

Dentro do cluster, o DW fica em `cidades-pg-rw.cidades.svc:5432`. Os detalhes de
como o pg_duckdb lê o lake (`read_parquet`, secret por usuário) são os mesmos de
`infra/playbooks/postgres/README.md`.

### 3. Airflow

```bash
infra/k8s/criar-secrets.sh airflow [extra.env]
helm upgrade --install airflow apache-airflow/airflow --version 1.22.0 \
  -n cidades -f infra/k8s/airflow/values.yaml --timeout 15m
```

O código vem do repositório inteiro pelo git-sync (`/opt/airflow/dags/repo`,
`AIRFLOW_REPO_BASE`). Para publicar uma versão nova das DAGs, basta o merge na
branch de `dags.gitSync.branch`. Mudanças no `requirements.txt` ou no
`Dockerfile` pedem imagem nova e outro `defaultAirflowTag`.

Login: `airflow`, com a senha do Secret `airflow-admin`:

```bash
kubectl -n cidades get secret airflow-admin -o jsonpath='{.data.password}' | base64 -d
kubectl -n cidades port-forward svc/airflow-api-server 8080:8080
```

As Variables são importadas como nas VMs (`airflow variables import`, dentro do
pod do api-server). A connection `postgres_default` já vem do Secret
`cidades-airflow-env`.
