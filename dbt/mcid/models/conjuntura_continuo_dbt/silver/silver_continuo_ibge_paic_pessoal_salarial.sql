{{ config(materialized="table") }}

-- Silver do conjuntura contínuo: PAIC pessoal/salarial (IBGE).
-- pg_duckdb lê o parquet tipado da staging (MinIO). Full-refresh.
select *
from read_parquet('s3://data-lake-mcid/staging/ibge/paic_pessoal_salarial.parquet')
