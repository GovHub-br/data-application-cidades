{{ config(materialized='table') }}

-- Bronze do conjuntura contínuo: crédito imobiliário / PIB (BACEN).
-- Espelho fiel do parquet de staging, sem transformação. O caminho do
-- arquivo é declarado em `sources.yml` e resolvido pelo macro `fonte_lake()`,
-- que também registra a dependência na linhagem. Achatamento e tipagem ficam
-- na silver.

select * from {{ fonte_lake('bacen_credito_pib') }}
