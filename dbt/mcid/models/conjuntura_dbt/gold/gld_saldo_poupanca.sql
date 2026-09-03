{{ config(materialized='table') }}

-- Gold do conjuntura contínuo: Poupança SBPE — captação líquida e saldo.
-- Página 5. Fonte: ABECIP (automatizado). Bate exato com o boletim
-- (captação líquida 12m/2025 = -R$ 63,0 bi).

select
    data_referencia::date as data_referencia,
    deposito,
    retirada,
    captacao_liquida_valor,
    captacao_liquida_pct,
    saldo
from {{ ref('slv_abecip_poupanca_sbpe') }}
order by data_referencia desc
