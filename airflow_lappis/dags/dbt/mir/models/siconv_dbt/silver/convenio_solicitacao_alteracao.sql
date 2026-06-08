{{ config(materialized="table") }}

with
    convenio as (
        select *
        from {{ ref("convenio") }}
    ),
    solicitacao_agg as (
        select
            nr_convenio,
            count(id_solicitacao) as qtd_solicitacoes,
            string_agg(distinct situacao_solicitacao, ', ') as situacoes,
            string_agg(distinct objeto_solicitacao, ' | ') as objetos_solicitacao,
            min(data_solicitacao) as primeira_solicitacao,
            max(data_solicitacao) as ultima_solicitacao
        from {{ ref("solicitacao_alteracao") }}
        group by nr_convenio
    )

select
    c.*,
    s.qtd_solicitacoes,
    s.situacoes,
    s.objetos_solicitacao,
    s.primeira_solicitacao,
    s.ultima_solicitacao
from convenio c
left join solicitacao_agg s on c.nr_convenio = s.nr_convenio