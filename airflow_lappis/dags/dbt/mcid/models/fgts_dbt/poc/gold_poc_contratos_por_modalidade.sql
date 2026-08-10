{#
    PoC -- gold. Postgres puro, sem nenhuma referencia a MinIO ou pg_duckdb.

    Este model e a prova de que a camada de servico nao precisa saber de onde
    o dado veio: ele le uma tabela local e agrega. Se amanha a bronze virar
    materializada, ou voltar para tabelas carregadas por COPY, este arquivo
    nao muda uma virgula.
#}

{{
    config(
        enabled=var('fgts_poc_enabled', false),
        materialized='table',
        schema='fgts_poc',
        tags=['fgts_poc']
    )
}}

with contratos as (
    select * from {{ ref('silver_poc_contratos') }}
)

select
    ano_orcamento,
    modalidade_descricao,
    area_descricao,

    count(*)                                as qtd_contratos,
    count(distinct cod_empreendimento)      as qtd_empreendimentos,
    count(distinct cod_tomador)             as qtd_tomadores,

    sum(valor_contratado)                   as valor_contratado_total,
    sum(valor_investimento)                 as valor_investimento_total,
    avg(valor_contratado)::numeric(15, 2)   as valor_contratado_medio,

    min(data_assinatura)                    as primeira_assinatura,
    max(data_assinatura)                    as ultima_assinatura

from contratos
where ano_orcamento is not null
group by
    ano_orcamento,
    modalidade_descricao,
    area_descricao
order by
    ano_orcamento desc,
    valor_contratado_total desc nulls last
