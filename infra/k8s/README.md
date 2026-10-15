# Kubernetes

A mesma infra dos playbooks de `infra/playbooks/`, num cluster Kubernetes: RustFS
(lake), PostgreSQL + pg_duckdb (data warehouse), OpenMetadata (catálogo) e
Airflow. Tudo fica no namespace `cidades`.

| Pasta | O que é | Como sobe |
| --- | --- | --- |
| `rustfs/` | RustFS 1.0.1, standalone | chart oficial `rustfs/rustfs` 1.0.1 |
| `postgres/` | PostgreSQL 15 + pg_duckdb 1.2.0 | operator CloudNativePG + imagem própria (`Dockerfile`) |
| `openmetadata/` | OpenMetadata 1.13.3 + Elasticsearch 9.3.0 | chart oficial `open-metadata/openmetadata` 1.13.3 + `elasticsearch.yaml` + Job `bootstrap.yaml` |
| `airflow/` | Airflow 3.3.2, LocalExecutor | chart oficial `apache-airflow/airflow` 1.22.0 + imagem de `infra/docker/airflow/` |
| `criar-secrets.sh` | todos os Secrets (senhas geradas no cluster) | `./criar-secrets.sh rustfs\|postgres\|openmetadata\|airflow` |

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
helm repo add open-metadata https://helm.open-metadata.org
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

### 3. OpenMetadata

```bash
infra/k8s/criar-secrets.sh openmetadata
kubectl apply -f infra/k8s/openmetadata/elasticsearch.yaml
kubectl -n cidades rollout status statefulset/elasticsearch --timeout=10m
helm upgrade --install openmetadata open-metadata/openmetadata --version 1.13.3 \
  -n cidades -f infra/k8s/openmetadata/values.yaml --wait --timeout 20m
kubectl apply -f infra/k8s/openmetadata/bootstrap.yaml
kubectl -n cidades wait job/openmetadata-bootstrap --for=condition=Complete --timeout=10m
```

Como no playbook de VM: o banco é o `Database` `openmetadata` do `cluster.yaml`, a
busca é o Elasticsearch de `elasticsearch.yaml` (com `node.store.allow_mmap=false`,
que dispensa ajustar `vm.max_map_count` nos nós) e a ingestão roda no Airflow do
projeto, então o cliente de pipeline e os CronJobs do chart ficam desligados. A
primeira subida roda a migração do banco num init container e leva uns 10 minutos.

O Job `bootstrap.yaml` troca a senha padrão do admin pela do Secret
`openmetadata-admin` e grava o Secret `openmetadata-airflow-env` (URL e token do
ingestion-bot), que o Airflow lê como opcional. Login na UI:

```bash
kubectl -n cidades get secret openmetadata-admin -o jsonpath='{.data.password}' | base64 -d
kubectl -n cidades port-forward svc/openmetadata 8585:8585   # admin@open-metadata.org
```

> **Pendência no código das DAGs.** As recipes de conector
> (`helpers/openmetadata/recipes/`) usam `${OM_HOST}` literalmente e só funcionam
> com a URL terminada em `/api`; `scripts/governance/governanca_comum.py` monta
> `{url}/api/v1` e só funciona sem. O `OPENMETADATA_URL` segue a convenção do
> projeto (sem `/api`), e até o código normalizar o `OM_HOST` as tarefas de
> conector da `openmetadata_ingestion_dag` falham com
> `'Response' object is not subscriptable`.

O Elasticsearch para de alocar índices com o disco do PVC acima de 90%; mantenha
folga no `storage` de `elasticsearch.yaml`.

### 4. Airflow

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
`cidades-airflow-env`, e as variáveis `OPENMETADATA_*` do Secret
`openmetadata-airflow-env`. Se o OpenMetadata for instalado depois do Airflow,
reinicie os pods do Airflow para eles lerem o Secret
(`kubectl -n cidades rollout restart deploy,statefulset -l release=airflow`).
