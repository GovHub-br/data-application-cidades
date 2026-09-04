{{ config(materialized='table') }}

-- Silver do conjuntura contínuo: Crédito Imobiliário / PIB (%).
-- Página 4 do boletim. Fonte: BCB Olinda MercadoImobiliario
-- (indicador indices_imobiliario_pib_br). Série mensal.
-- Lê o parquet tipado da staging (pg_duckdb). Full-refresh.

select data::date as data, valor::numeric as valor
from {{ ref('bnz_bacen_credito_pib') }}
