{{ config(materialized='table') }}

-- Prata do conjuntura: Crédito Imobiliário / PIB (%).
-- Página 4 do boletim. Fonte: BCB Olinda MercadoImobiliario
-- (indicador indices_imobiliario_pib_br). Série mensal.
-- Lê o parquet tipado da staging (pg_duckdb). Full-refresh.

-- Ingestão nova (plugins/ingestion, 10/2026): a staging guarda o JSON do Olinda
-- como veio (`Data`, `Info`, `Valor`); o renome é aqui.
select "Data"::date as data, "Valor"::numeric as valor
from {{ ref('bronze_bacen_credito_pib') }}
