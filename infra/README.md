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

## Layout

```text
infra/
├── airflow/              # airflow.cfg usado no ambiente local
├── docker/
│   ├── airflow/          # imagem principal do Airflow
│   ├── superset/         # imagem do Superset com drivers do PostgreSQL
│   └── postgres/         # scripts de init do Postgres
├── env/                  # exemplos de variaveis de ambiente
└── docker-compose.yml
```
