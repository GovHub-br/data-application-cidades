{{ config(materialized='table') }}

-- Bronze do conjuntura: Planilha Interativa da MRV (dados operacionais).
-- Espelho fiel do parquet de staging, sem transformação. Ainda sem prata:
-- nenhum ouro lê a MRV (o boletim usa o dado manual de balanços); lançamentos
-- e vendas viram recorte na prata quando alguém precisar.

select * from {{ fonte_lake('mrv_planilha_interativa', 'lake_staging') }}
