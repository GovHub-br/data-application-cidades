{{ config(materialized='table') }}

-- Bronze do conjuntura: FipeZap locação (FIPE).
-- Espelho fiel do parquet de staging, sem transformação. O caminho do
-- arquivo é declarado em `sources.yml` e resolvido pelo macro `fonte_lake()`,
-- que também registra a dependência na linhagem. Achatamento e tipagem ficam
-- na prata.

select * from {{ fonte_lake('fipezap_locacao', 'lake_staging') }}
