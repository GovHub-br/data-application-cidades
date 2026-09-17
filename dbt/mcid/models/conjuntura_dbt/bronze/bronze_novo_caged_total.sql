{{ config(materialized='table') }}

-- Bronze do conjuntura: Novo CAGED — total construção.
-- Espelho fiel do parquet de staging, sem transformação. O caminho do
-- arquivo é declarado em `sources.yml` e resolvido pelo macro `fonte_lake()`,
-- que também registra a dependência na linhagem. Achatamento e tipagem ficam
-- na prata.

select * from {{ fonte_lake('novo_caged_total', 'lake_staging') }}
