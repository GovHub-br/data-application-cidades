{{ config(materialized='table') }}

-- Bronze do conjuntura: SINAPI (IBGE).
-- Espelho fiel do parquet de staging, sem transformação: pg_duckdb lê via
-- read_parquet e materializa no banco. Achatamento e tipagem ficam na prata.

select * from {{ fonte_lake('ibge_sinapi', 'lake_staging') }}
