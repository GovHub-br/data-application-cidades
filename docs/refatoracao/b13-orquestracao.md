# B13 · Orquestração dos DAGs

> Parte do [guia das refatorações](README.md) · Prioridade P2 · Status: issue aberta · Issue: [#260](https://github.com/GovHub-br/data-application-cidades/issues/260) · PR: —

## Problema

- **Slots presos em polling.** O `conjuntura_dag` dispara 13 DAGs com `wait_for_completion=True` e `poke_interval=60`, e segura 13 slots de worker enquanto espera (`dags/conjuntura/conjuntura_dag.py:92-99`). Decidido no ADR-0013: a `conjuntura_dag` sai, e cada fonte roda no próprio cron.
- **Runs sobrepostos do dbt.** O `mcid_cosmos_dag` não tem `max_active_runs`, e é disparado pelo cron e pelo `minio_transform_dag` (`dags/data_ingest/minio/minio_transform_dag.py:103-110`).
- **Lista incompleta.** abecip_financiamentos e abecip_instituicoes alimentam a staging, mas faltam em `INGEST_DAG_IDS`.
- **Variables sem versionamento nem validação.** `dynamic_schedules`, `IBGE_CONFIGURACOES` e `BACEN_SERIES` não estão no repositório. Uma vírgula sobrando no JSON do `IBGE_CONFIGURACOES` derruba o DAG no parse. O `infra/playbooks/airflow/README.md:143` manda importar um `variables.json` que não existe.

## Correção proposta

- `deferrable=True` nos `TriggerDagRunOperator`.
- `max_active_runs=1` no `mcid_cosmos_dag`.
- Completar a `INGEST_DAG_IDS`.
- Versionar as Variables, com validação de esquema e leitura dentro da task.

## Relação

Absorvido pela [F2](f2-ingestao.md). A dupla execução do conjuntura no dbt está na [F3](f3-dbt.md).
