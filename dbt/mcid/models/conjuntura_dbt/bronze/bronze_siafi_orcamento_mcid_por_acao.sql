{{ config(materialized='table') }}

-- Bronze do conjuntura: orçamento do MCid por ação (SIAFI/Tesouro Gerencial).
-- Espelho fiel do parquet de staging, sem transformação. Ainda sem prata:
-- nenhum ouro lê este relatório. As duas primeiras linhas são o subcabeçalho
-- das colunas de valor do relatório.

select * from {{ fonte_lake('siafi_orcamento_mcid_por_acao', 'lake_staging') }}
