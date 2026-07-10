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

Ou chame o Compose diretamente:

```bash
docker compose -f infra/docker-compose.yml up postgres airflow superset
```

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
