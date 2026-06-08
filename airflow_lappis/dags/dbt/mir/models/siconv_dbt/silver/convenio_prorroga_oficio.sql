{{ config(materialized="table") }}

with
    convenio as (
        select *
        from {{ ref("convenio") }}
    ),
    prorroga_agg as (
        select
            nr_convenio,
            count(nr_prorroga) as qtd_prorrogacoes,
            sum(dias_prorroga) as total_dias_prorrogado,
            min(dt_inicio_prorroga) as primeira_prorroga,
            max(dt_fim_prorroga) as ultima_prorroga,
            string_agg(distinct sit_prorroga, ', ') as situacoes_prorroga
        from {{ ref("prorroga_oficio") }}
        group by nr_convenio
    )

select
    c.*,
    p.qtd_prorrogacoes,
    p.total_dias_prorrogado,
    p.primeira_prorroga,
    p.ultima_prorroga,
    p.situacoes_prorroga
from convenio c
left join prorroga_agg p on c.nr_convenio = p.nr_convenio