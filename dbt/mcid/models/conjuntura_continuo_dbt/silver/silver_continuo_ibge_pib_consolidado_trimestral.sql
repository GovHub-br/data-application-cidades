{{ config(materialized="table") }}

-- Silver do conjuntura contínuo: PIB consolidado trimestral (IBGE).
-- pg_duckdb lê o parquet tipado da staging (MinIO). Full-refresh.
select *
from
    read_parquet(
        's3://data-lake-mcid/staging/ibge/pib_consolidado_trimestral_bruto.parquet'
    )
