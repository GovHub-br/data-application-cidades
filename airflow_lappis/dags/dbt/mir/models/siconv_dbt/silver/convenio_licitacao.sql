{{ config(materialized="table") }}

with
    convenio as (
        select *
        from {{ ref("convenio") }}
    ),
    licitacao_agg as (
        select
            nr_convenio,
            count(id_licitacao) as qtd_licitacoes,
            sum(valor_licitacao) as total_contratado,
            string_agg(distinct modalidade_licitacao, ', ') as modalidades,
            string_agg(distinct status_licitacao, ', ') as status,
            string_agg(distinct situacao_aceite_processo_execu, ', ') as situacoes_aceite
        from {{ ref("licitacao") }}
        group by nr_convenio
    )

select
    c.*,
    l.qtd_licitacoes,
    l.total_contratado,
    l.modalidades,
    l.status,
    l.situacoes_aceite
from convenio c
left join licitacao_agg l on c.nr_convenio = l.nr_convenio