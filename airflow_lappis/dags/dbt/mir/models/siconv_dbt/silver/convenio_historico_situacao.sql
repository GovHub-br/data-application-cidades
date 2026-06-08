{{ config(materialized="table") }}

with
    convenio as (
        select *
        from {{ ref("convenio") }}
    ),
    historico_agg as (
        select
            nr_convenio,
            count(*) as qtd_mudancas_situacao,
            max(dias_historico_sit) as max_dias_em_situacao,
            min(dia_historico_sit) as primeira_mudanca,
            max(dia_historico_sit) as ultima_mudanca,
            string_agg(distinct historico_sit, ', ') as todas_situacoes,
            bool_or(historico_sit = 'INADIMPLENTE') as teve_inadimplencia,
            bool_or(historico_sit = 'CONVENIO_RESCINDIDO') as foi_rescindido,
            bool_or(historico_sit = 'CONVENIO_ANULADO') as foi_anulado
        from {{ ref("historico_situacao") }}
        group by nr_convenio
    )

select
    c.*,
    h.qtd_mudancas_situacao,
    h.max_dias_em_situacao,
    h.primeira_mudanca,
    h.ultima_mudanca,
    h.todas_situacoes,
    h.teve_inadimplencia,
    h.foi_rescindido,
    h.foi_anulado
from convenio c
left join historico_agg h on c.nr_convenio = h.nr_convenio