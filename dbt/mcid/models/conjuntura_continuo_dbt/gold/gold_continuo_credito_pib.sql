{{ config(materialized="table") }}

-- Gold do conjuntura contínuo: Crédito Imobiliário / PIB (%). Página 4.
-- Fonte: BCB Olinda MercadoImobiliario (automatizado).
select data, valor as credito_imobiliario_pib_pct
from {{ ref("silver_continuo_bacen_credito_pib") }}
order by data desc
