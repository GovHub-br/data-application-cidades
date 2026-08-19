{{ config(materialized="table") }}

select
    cast(data_referencia as date) as data_referencia,
    cast(deposito as numeric) as deposito,
    cast(retirada as numeric) as retirada,
    cast(captacao_liquida_valor as numeric) as captacao_liquida_valor,
    cast(captacao_liquida_pct as numeric) as captacao_liquida_pct,
    cast(rendimento as numeric) as rendimento,
    cast(saldo as numeric) as saldo,
    cast(fonte as varchar) as fonte,
    cast(dt_ingest as timestamp) as dt_ingest
from {{ source("abecip", "poupanca_sbpe_mensal") }}
