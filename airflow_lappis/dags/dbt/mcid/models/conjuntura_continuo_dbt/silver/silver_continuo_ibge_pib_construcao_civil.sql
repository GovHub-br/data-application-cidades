{{ config(materialized='table') }}

-- Silver do conjuntura contínuo.
-- pg_duckdb lê o parquet TIPADO da staging (MinIO) direto do Postgres via
-- read_parquet. Como o parquet já sai tipado da ingestão (task
-- gera_parquet_tipado do ibge_ingest_dag), aqui a silver é passthrough.
-- Full-refresh: cada run reconstrói a tabela a partir do parquet atual.

select *
from read_parquet('s3://data-lake-mcid/staging/ibge/pib_construcao.parquet')
