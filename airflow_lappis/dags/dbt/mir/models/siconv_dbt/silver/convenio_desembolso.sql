{{ config(materialized="table") }}

with
    convenio as (
        select *
        from {{ ref("convenio") }}
    ),
    desembolso_agg as (
        select 
            nr_convenio,
            sum(vl_desembolsado) as total_desembolsado_real,
            max(qtd_dias_sem_desembolso) as max_dias_sem_desembolso,
            count(id_desembolso) as qtd_desembolsos
        from {{ ref("desembolso") }}
        group by nr_convenio
    )

select
    c.*,
    d.total_desembolsado_real,
    d.max_dias_sem_desembolso,
    d.qtd_desembolsos
from convenio c
left join desembolso_agg d on c.nr_convenio = d.nr_convenio