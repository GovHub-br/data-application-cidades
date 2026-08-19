{{ config(materialized='table') }}

-- Silver do conjuntura contínuo: Índice FipeZap de locação (FIPE).
-- pg_duckdb lê o parquet tipado da staging (MinIO) direto do Postgres.
-- Parquet já sai tipado da ingestão (Etapa 02), então a silver é passthrough.
-- Full-refresh: cada run reconstrói a tabela a partir do parquet atual.

select *
from read_parquet('s3://data-lake-mcid/staging/fipe/indice_locacao.parquet')
