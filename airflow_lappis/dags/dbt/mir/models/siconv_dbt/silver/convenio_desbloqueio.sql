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
            count(nr_ob) as qtd_ordens_bancarias,
            min(data_cadastro) as primeiro_desbloqueio,
            max(data_cadastro) as ultimo_desbloqueio
        from {{ ref("desbloqueio") }}
        group by nr_convenio
    )

select
    c.*,
    d.total_desbloqueado,
    d.total_bloqueado,
    d.total_valor_desbloqueio,
    d.qtd_ordens_bancarias,
    d.primeiro_desbloqueio,
    d.ultimo_desbloqueio
from convenio c
left join desbloqueio_agg d on c.nr_convenio = d.nr_convenio