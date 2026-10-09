{{ config(materialized='table') }}

-- Bronze do conjuntura: notas de empenho de emendas parlamentares (SIAFI).
-- Espelho fiel do parquet de staging, sem transformação. Ainda sem prata:
-- nenhum ouro lê este relatório. As duas primeiras linhas são o subcabeçalho
-- das colunas de valor do relatório.

select * from {{ fonte_lake('siafi_notas_empenho_emendas_parlamentares', 'lake_staging') }}
