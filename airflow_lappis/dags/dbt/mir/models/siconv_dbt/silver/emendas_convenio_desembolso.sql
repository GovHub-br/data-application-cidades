{{ config(materialized="table") }}

with
    emendas_convenio_proposta as (
        select *
        from {{ ref("emendas_convenio_proposta") }}
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
    ec.*,
    d.total_desembolsado_real,
    d.max_dias_sem_desembolso,
    d.qtd_desembolsos
from emendas_convenio_proposta ec
left join desembolso_agg d on ec.nr_convenio = d.nr_convenio