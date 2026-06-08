{{ config(materialized="table") }}

with
    convenio as (
        select *
        from {{ ref("convenio") }}
    ),
    empenho_agg as (
        select
            nr_convenio,
            sum(valor_empenho) as total_empenhado,
            count(id_empenho) as qtd_empenhos,
            string_agg(distinct natureza_despesa::text, ', ') as naturezas_despesa,
            string_agg(distinct desc_situacao_empenho, ', ') as situacoes_empenho
        from {{ ref("empenho") }}
        group by nr_convenio
    )

select
    c.*,
    e.total_empenhado,
    e.qtd_empenhos,
    e.naturezas_despesa,
    e.situacoes_empenho
from convenio c
left join empenho_agg e on c.nr_convenio = e.nr_convenio