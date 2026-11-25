{{ config(materialized='table') }}

-- Prata do conjuntura: Crédito Imobiliário / PIB (%).
-- Página 4 do boletim. Fonte: BCB Olinda MercadoImobiliario
-- (indicador indices_imobiliario_pib_br). Série mensal.
-- A staging é texto (ADR-0002): a tipagem é aqui.
-- A bronze guarda todas as cargas; cada carga traz a série inteira, então fica a última.
select data::date as data, valor::numeric as valor
from {{ ref('bronze_bacen_credito_pib') }}
where {{ ultima_carga(ref('bronze_bacen_credito_pib')) }}
