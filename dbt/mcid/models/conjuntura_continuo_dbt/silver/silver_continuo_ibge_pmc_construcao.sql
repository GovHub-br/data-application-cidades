{{ config(materialized="table") }}

-- Silver do conjuntura contínuo: PMC construção (IBGE).
-- pg_duckdb lê o parquet tipado da staging (MinIO). Full-refresh.
select *
from read_parquet('s3://data-lake-mcid/staging/ibge/ibge_pmc_construcao.parquet')
