# Infra

Arquivos de execucao local do projeto.

## Compose

Use os atalhos da raiz:

```bash
make compose-config
make up
make logs-airflow
make down
```

Ou chame o Compose diretamente, a partir da raiz do repositorio:

```bash
docker compose --env-file .env -f infra/docker-compose.yml up -d
```

O `--env-file .env` e necessario: o Compose carrega o `.env` do diretorio do
arquivo de compose (aqui, `infra/`), e o `.env` do projeto fica na raiz. Sem
ele, variaveis como `AIRFLOW_HOME` chegam vazias e os volumes sao montados em
caminhos errados. Os alvos do Makefile ja passam a flag.

O `name: data-application-cidades` no compose fixa o nome do projeto. Sem ele o
Compose usaria o nome do diretorio (`infra`) e criaria containers e volumes
separados dos que ja existem.

## Airflow 3

O login da UI e da API e `airflow` / `airflow` (SimpleAuthManager, senha em
`infra/airflow/simple_auth_manager_passwords.json`). O healthcheck fica em
`/api/v2/monitor/health`.

Quem ja tinha o ambiente em Airflow 2.8.1: o primeiro `make up` migra o banco
de metadados do volume `postgres-db` para o schema do 3.x, e essa migracao nao
volta. Para ter um ambiente limpo, `make down` e apague o volume antes.

## Layout

```text
infra/
├── airflow/              # airflow.cfg usado no ambiente local
├── docker/
│   ├── airflow/          # imagem principal do Airflow
│   ├── superset/         # imagem do Superset com drivers do PostgreSQL
│   └── postgres/         # scripts de init do Postgres
├── env/                  # exemplos de variaveis de ambiente
├── playbooks/            # Ansible: uma VM por ferramenta (producao)
│   ├── rustfs/           # RustFS, o object storage S3 do lake
│   ├── postgres/         # PostgreSQL + pg_duckdb (data warehouse)
│   ├── openmetadata/     # OpenMetadata (catalogo de dados)
│   └── airflow/          # Airflow via Docker Compose
├── k8s/                  # a mesma infra em Kubernetes (Helm + CloudNativePG)
└── docker-compose.yml
```

## Playbooks (producao)

`infra/playbooks/` sobe a infra de producao em tres VMs Debian 12, uma por
ferramenta, via Ansible. Nao faz parte do Compose local. Rode na ordem abaixo,
sempre da mesma maquina: cada playbook le as senhas que o anterior gravou em
`~/.clusterlab/credentials/`.

1. `rustfs/`: instala o RustFS.
2. No RustFS, crie o bucket `data-lake-mcid` e o usuario `app-cidades` (ver
   `postgres/README.md`, "Como rodar").
3. `postgres/`: instala o Postgres + pg_duckdb e liga o pg_duckdb ao lake.
4. `openmetadata/`: instala o catalogo, com o banco na VM do Postgres.
5. `airflow/`: instala o Airflow apontando para o Postgres, o lake e o
   OpenMetadata (este ultimo e opcional).

| Pasta | O que instala |
| --- | --- |
| `rustfs/` | RustFS, storage de objetos compativel com S3 (o data lake) |
| `postgres/` | PostgreSQL 18 + pg_duckdb, ja com o secret S3 do lake (`read_parquet('s3://...')`) |
| `openmetadata/` | OpenMetadata 2.0.7 + Elasticsearch, com Docker Compose; a ingestao roda no Airflow |
| `airflow/` | Airflow 3 com Docker Compose, imagem de `infra/docker/airflow/Dockerfile` |

Cada pasta e autocontida (inventario, `group_vars/` e README proprios).

## Kubernetes (producao, alternativa as VMs)

`infra/k8s/` sobe a mesma infra num cluster Kubernetes, no namespace `cidades`:
RustFS pelo chart oficial, Postgres + pg_duckdb pelo operator CloudNativePG
(com imagem propria, `infra/k8s/postgres/Dockerfile`), OpenMetadata e Airflow
pelos charts oficiais. Os Secrets saem de `infra/k8s/criar-secrets.sh`. Passo a passo em
`infra/k8s/README.md`.
