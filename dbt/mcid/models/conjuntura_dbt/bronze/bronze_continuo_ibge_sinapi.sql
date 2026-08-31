{{ config(materialized='table') }}

-- Bronze do conjuntura contínuo: SINAPI (IBGE).
-- Espelho fiel do parquet de staging, sem transformação: pg_duckdb lê via
-- read_parquet e materializa no banco. Achatamento e tipagem ficam na silver.

select * from {{ fonte_lake('ibge_sinapi') }}
