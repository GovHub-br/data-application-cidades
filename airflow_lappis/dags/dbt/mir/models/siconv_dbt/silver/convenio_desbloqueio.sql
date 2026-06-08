{{ config(materialized="table") }}

with
    convenio as (
        select *
        from {{ ref("convenio") }}
    ),
    desbloqueio_agg as (
        select
            nr_convenio,
            sum(vl_desbloqueado) as total_desbloqueado,
            sum(vl_bloqueado) as total_bloqueado,
            sum(vl_total_desbloqueio) as total_valor_desbloqueio,
            count(nr_ob) as qtd_ordens_bancarias
        from {{ ref("desbloqueio") }}
        group by nr_convenio
    )

select
    c.*,
    d.total_desbloqueado,
    d.total_bloqueado,
    d.total_valor_desbloqueio,
    d.qtd_ordens_bancarias
from convenio c
left join desbloqueio_agg d on c.nr_convenio = d.nr_convenio