{{ config(materialized="table") }}

with
    proposta as (
        select *
        from {{ ref("proposta") }}
    ),
    historico_agg as (
        select
            id_proposta,
            count(*) as qtd_mudancas_situacao,
            max(dias_historico_sit) as max_dias_em_situacao,
            min(dia_historico_sit) as primeira_mudanca,
            max(dia_historico_sit) as ultima_mudanca,
            string_agg(distinct historico_sit, ', ') as situacoes
        from {{ ref("historico_situacao") }}
        group by id_proposta
    )

select
    p.*,
    h.qtd_mudancas_situacao,
    h.max_dias_em_situacao,
    h.primeira_mudanca,
    h.ultima_mudanca,
    h.situacoes
from proposta p
left join historico_agg h on p.id_proposta = h.id_proposta