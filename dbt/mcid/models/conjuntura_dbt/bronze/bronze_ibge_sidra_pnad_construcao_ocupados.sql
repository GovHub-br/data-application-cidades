{{ config(materialized='table') }}

-- Bronze do conjuntura: PNAD Contínua, construção (ocupados), via SIDRA.
-- Espelho fiel do parquet de staging, sem transformação. Ainda sem prata: o
-- boletim lê o PNAD da API v3 (`bronze_ibge_pnad_construcao_ocupados`); esta é a
-- reserva para quando a v3 falhar. O primeiro registro da SIDRA é o cabeçalho.

select * from {{ fonte_lake('ibge_sidra_pnad_construcao_ocupados', 'lake_staging') }}
