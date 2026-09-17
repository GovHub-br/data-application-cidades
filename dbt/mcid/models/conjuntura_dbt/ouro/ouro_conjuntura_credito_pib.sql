{{ config(materialized='table') }}

-- Ouro do conjuntura: Crédito Imobiliário / PIB (%). Página 4.
-- Fonte: BCB Olinda MercadoImobiliario (automatizado).

select
    data,
    valor as credito_imobiliario_pib_pct
from {{ ref('prata_conjuntura_bacen_credito_pib') }}
order by data desc
