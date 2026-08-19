{{ config(materialized='table') }}

-- Silver do conjuntura contínuo: PNADC rendimento domiciliar real (IBGE).
-- pg_duckdb lê o parquet tipado da staging (MinIO). Full-refresh.

select *
from read_parquet('s3://data-lake-mcid/staging/ibge/pnadc_rendimento_domiciliar_real.parquet')
