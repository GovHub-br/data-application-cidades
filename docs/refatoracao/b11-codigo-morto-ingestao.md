# B11 · Código morto da ingestão

> Parte do [guia das refatorações](README.md) · Prioridade P2 · Status: issue aberta · Issue: [#258](https://github.com/GovHub-br/data-application-cidades/issues/258) · PR: —

## O que remover

- **`ClientPostgresDB`** (`plugins/cliente_postgres.py:183-569`):
  - métodos que consultam schemas removidos no #237: `compras_gov`, `transfere_gov.*`, `pessoas.*`, `siafi.remove_duplicates` e `siorg.get_codigo_unidade`. Nenhum é chamado;
  - `alter_table`, `drop_table_if_exists`, `insert_csv_data`, `execute_non_query` e `TYPE_MAP`/`_get_column_type` (`:14-27`), que também não têm uso.
- **`cliente_email`:** `fetch_email_with_csv`, `extract_csv_from_payload` e `fetch_and_process_email_csv_attachment` (`:86,104,173`).
- **`cliente_minio`:** `s3_staging_uri` (`:70`).
- **`IngestorLake.subir_raw`** (`plugins/ingestor_lake.py:128`).
- **`ClienteSftp.baixar_para_tempfile`**, reimplementado em `scripts/sftp_para_minio.py:406`.
- **Scripts sem uso:** `scripts/sftp_ingest.py` (loader SFTP→Postgres antigo) e `scripts/sftp_rename_tables.py` (migração pontual).
- **Os `typers=` mortos** em bacen_sgs, credito_pib, incc_m e sidra. Eles geram um warning em todo run.
- **Branches mortos:**
  - o `except requests...` em `cliente_fipe.py:129`, porque o `ClienteBase` usa httpx;
  - `cliente_base.py:92`, que é inalcançável.
- **Configuração órfã:** as chaves `AIRFLOW_CONN_MINIO_LAKE`, `AIRFLOW_CONN_HTTP_BACEN`, `AIRFLOW_CONN_HTTP_SINDUSCON` e `INGESTION_STORAGE_PREFIX`, que nada referencia.
- **Locais, não versionados:** os `__pycache__` de refactors antigos em `plugins/` e `plugins/ingestion/`.

## Critério de aceite

- `grep` sem referências aos símbolos removidos.
- Os testes e o DagBag continuam verdes.

## Relação

Absorvido pela [F2](f2-ingestao.md). Pode ir antes, como limpeza avulsa, se ajudar.
