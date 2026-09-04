{{ config(materialized="table") }}

-- Silver do conjuntura contínuo: PNADC população por decil de renda (IBGE).
-- pg_duckdb lê o parquet tipado da staging (MinIO). Full-refresh.
select *
from read_parquet('s3://data-lake-mcid/staging/ibge/pnadc_populacao_decis_renda.parquet')
