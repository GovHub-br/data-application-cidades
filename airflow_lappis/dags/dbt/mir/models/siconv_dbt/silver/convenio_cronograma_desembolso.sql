{{ config(materialized="table") }}

with
    convenio as (
        select *
        from {{ ref("convenio") }}
    ),
    cronograma_agg as (
        select
            nr_convenio,
            sum(valor_parcela_crono_desembolso) as total_previsto_cronograma,
            count(nr_parcela_crono_desembolso) as qtd_parcelas_previstas
        from {{ ref("cronograma_desembolso") }}
        group by nr_convenio
    ),
    desembolso_agg as (
        select
            nr_convenio,
            sum(vl_desembolsado) as total_desembolsado_real,
            count(id_desembolso) as qtd_desembolsos
        from {{ ref("desembolso") }}
        group by nr_convenio
    )

select
    c.*,
    cr.total_previsto_cronograma,
    cr.qtd_parcelas_previstas,
    d.total_desembolsado_real,
    d.qtd_desembolsos,
    cr.total_previsto_cronograma - d.total_desembolsado_real as diferenca_previsto_executado
from convenio c
left join cronograma_agg cr on c.nr_convenio = cr.nr_convenio
left join desembolso_agg d on c.nr_convenio = d.nr_convenio