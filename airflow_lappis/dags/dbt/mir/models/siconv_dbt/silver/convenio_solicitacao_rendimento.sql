{{ config(materialized="table") }}

with
    convenio as (
        select *
        from {{ ref("convenio") }}
    ),
    rendimento_agg as (
        select
            nr_convenio,
            count(id_solicitacao_rend_aplicacao) as qtd_solicitacoes,
            sum(valor_solicitacao_rend_aplicacao) as total_solicitado,
            sum(valor_aprovado_solicitacao_rend_aplicacao) as total_aprovado,
            string_agg(distinct status_solicitacao_rend_aplicacao, ', ') as status_solicitacoes
        from {{ ref("solicitacao_rendimento_aplicacao") }}
        group by nr_convenio
    )

select
    c.*,
    r.qtd_solicitacoes,
    r.total_solicitado,
    r.total_aprovado,
    r.status_solicitacoes
from convenio c
left join rendimento_agg r on c.nr_convenio = r.nr_convenio