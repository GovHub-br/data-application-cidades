{{ config(materialized="table") }}

with
    financeira as (
        select
            apf,
            mes,
            pct_executado_financeiro
        from {{ ref("evolucao_financeira") }}
    ),

    fisica as (
        select
            apf,
            date_trunc('month', dt_alteracao_situacao) as mes_fisica,
            pct_obra_realizada
        from {{ ref("obra_mensal") }}
        where dt_alteracao_situacao is not null
    )

select
    coalesce(f.apf, o.apf) as apf,
    to_char(coalesce(f.mes, o.mes_fisica), 'YYYY-MM-DD') as mes,
    o.pct_obra_realizada,
    f.pct_executado_financeiro
from financeira f
full outer join fisica o
    on f.apf = o.apf
    and f.mes = o.mes_fisica
