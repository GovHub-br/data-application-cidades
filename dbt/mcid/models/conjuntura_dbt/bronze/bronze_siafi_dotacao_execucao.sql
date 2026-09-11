{{ config(materialized='table') }}

-- Bronze do conjuntura: dotação e execução (SIAFI).
-- Espelho fiel do parquet de staging, sem transformação. O caminho do
-- arquivo é declarado em `sources.yml` e resolvido pelo macro `fonte_lake()`,
-- que também registra a dependência na linhagem. Achatamento e tipagem ficam
-- na prata.

select * from {{ fonte_lake('siafi_dotacao_execucao', 'lake_staging') }}
