{{ config(materialized="table") }}

with
    convenio as (
        select *
        from {{ ref("convenio") }}
    ),
    contrapartida_agg as (
        select
            nr_convenio,
            sum(vl_ingresso_contrapartida) as total_depositado,
            count(*) as qtd_ingressos,
            min(dt_ingresso_contrapartida) as primeiro_ingresso,
            max(dt_ingresso_contrapartida) as ultimo_ingresso
        from {{ ref("ingresso_contrapartida") }}
        group by nr_convenio
    )

select
    c.*,
    ct.total_depositado,
    ct.qtd_ingressos,
    ct.primeiro_ingresso,
    ct.ultimo_ingresso,
    case
        when c.vl_contrapartida_conv = 0 then 'SEM CONTRAPARTIDA PREVISTA'
        when ct.total_depositado is null then 'NENHUM DEPÓSITO'
        when ct.total_depositado >= c.vl_contrapartida_conv then 'CUMPRIDO'
        else 'PARCIAL'
    end as situacao_contrapartida,
    case
        when c.vl_contrapartida_conv > 0 
        then round((ct.total_depositado / c.vl_contrapartida_conv * 100)::numeric, 1)
        else null
    end as percentual_depositado
from convenio c
left join contrapartida_agg ct on c.nr_convenio = ct.nr_convenio