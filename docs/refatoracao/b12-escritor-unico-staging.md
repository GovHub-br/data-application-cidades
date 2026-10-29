# B12 · Um escritor de staging

> Parte do [guia das refatorações](README.md) · Prioridade P2 · Status: a abrir · Issue: — · PR: —

## Problema

Hoje três caminhos de código gravam na staging, cada um com uma regra:

| Escritor | Onde | Regra |
|---|---|---|
| `registros_para_staging_parquet` | `plugins/ingestor_lake.py:46-69` | Tudo como texto; ignora `typers` |
| `gera_parquet_tipado` | `dags/data_ingest/ibge/ibge_ingest_dag.py:145-176` | Parquet tipado |
| pandas direto | `dags/data_ingest/abecip/abecip_instituicoes_ingest_dag.py:105-109` | Tipos inferidos pelo pandas |

O `scripts/raw_para_staging.py` é um quarto escritor (só texto, com colunas de linhagem). Ele já está tratado no [B1](b01-raw-para-staging-so-sftp.md).

## Correção proposta

Um escritor só, com a regra definida no ADR do contrato da staging ([F1](f1-adrs.md)).

## Depende de

[F1](f1-adrs.md) e [B1](b01-raw-para-staging-so-sftp.md).

## Relação

Absorvido pela [F2](f2-ingestao.md).
