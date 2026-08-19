{{ config(materialized='table') }}

-- Silver do conjuntura contínuo: PIM-PF Brasil (IBGE).
-- pg_duckdb lê o parquet tipado da staging (MinIO). Full-refresh.

select *
from read_parquet('s3://data-lake-mcid/staging/ibge/ibge_pim_pf_brasil.parquet')
