{{ config(materialized='table') }}

-- Bronze do conjuntura: PNAD Contínua, domicílios por condição de ocupação.
-- Espelho fiel do parquet de staging, sem transformação. Ainda sem prata:
-- nenhum ouro lê esta tabela.

select * from {{ fonte_lake('ibge_pnadc_habitacao_condicao', 'lake_staging') }}
