{{ config(materialized='table') }}

-- Silver do conjuntura contínuo: Crédito Imobiliário / PIB (%).
-- Página 4 do boletim. Fonte: BCB Olinda MercadoImobiliario
-- (indicador indices_imobiliario_pib_br). Série mensal.
-- Lê o parquet tipado da staging (pg_duckdb). Full-refresh.

select *
from read_parquet('s3://data-lake-mcid/staging/bacen/credito_imobiliario_pib.parquet')
