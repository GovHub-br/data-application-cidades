{{ config(materialized="table") }}

select
    data_referencia,
    deposito,
    retirada,
    captacao_liquida_valor,
    captacao_liquida_pct,
    rendimento,
    saldo,
    -- Captação líquida em bilhões
    round(captacao_liquida_valor::numeric / 1e3, 1) as captacao_liquida_bi,
    -- Acumulado 12 meses
    sum(captacao_liquida_valor) over (
        order by data_referencia rows between 11 preceding and current row
    ) as captacao_acum_12m,
    fonte,
    {{ add_metadata_timestamps("silver") }}
from {{ ref("bronze_abecip_poupanca_sbpe") }}
