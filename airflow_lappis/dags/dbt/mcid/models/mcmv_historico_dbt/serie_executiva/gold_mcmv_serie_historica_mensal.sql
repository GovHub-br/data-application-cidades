{{ config(materialized="table") }}

-- GOLD — serie historica mensal do MCMV (pre-2024) para analise preditiva.
--
-- Agrega a silver_mcmv_serie_executiva_historica por
-- (dt_referencia, fonte_familia, nivel_agregacao, uf, linha_ogu_fgts):
-- UH contratadas/entregues/concluidas, valores e subsidios. Niveis via
-- GROUPING SETS: 'nacional' e 'uf'.
--
-- IMPORTANTE — NAO somar entre fonte_familia: bases_relatorio_executivo e
-- min_cidades se sobrepoem no tempo (2014-2016). Para montar UMA serie continua,
-- filtrar por prioridade_familia (menor = preferencial), escolhendo por
-- (dt_referencia, uf) a familia de menor prioridade com dado. A consolidacao
-- fica a cargo do consumidor / de um mart posterior.
--
-- Alimenta: backtest do relogio, tendencia/sazonalidade/drift, e (via
-- linha_ogu_fgts) a substituicao futura do seed anual do piloto #118.
--
-- Target obrigatorio: staging_duckdb (gating em dbt_project.yml).

with

base as (
    select
        dt_referencia,
        year(dt_referencia) as ano,
        month(dt_referencia) as mes,
        fonte_familia,
        case fonte_familia
            when 'bases_relatorio_executivo' then 1
            when 'min_cidades' then 2
            when 'entrada_bb' then 3
            when 'bext' then 4
            else 9
        end as prioridade_familia,
        coalesce(uf, 'ND') as uf,
        coalesce(linha_ogu_fgts, 'Nao classificada') as linha_ogu_fgts,
        chave_natural,
        uh_contratadas,
        uh_entregues,
        uh_concluidas,
        uh_em_obras,
        valor_investimento,
        valor_emprestimo,
        valor_liberado,
        subsidio_fgts,
        subsidio_ogu
    from {{ ref("silver_mcmv_serie_executiva_historica") }}
    where dt_referencia is not null
),

agg as (
    select
        dt_referencia,
        ano,
        mes,
        fonte_familia,
        prioridade_familia,
        case when grouping(uf) = 0 then 'uf' else 'nacional' end as nivel_agregacao,
        case when grouping(uf) = 0 then uf else 'BR' end as uf,
        linha_ogu_fgts,
        count(distinct chave_natural) as n_registros,
        sum(uh_contratadas) as uh_contratadas,
        sum(uh_entregues) as uh_entregues,
        sum(uh_concluidas) as uh_concluidas,
        sum(uh_em_obras) as uh_em_obras,
        sum(valor_investimento) as valor_investimento,
        sum(valor_emprestimo) as valor_emprestimo,
        sum(valor_liberado) as valor_liberado,
        sum(subsidio_fgts) as subsidio_fgts,
        sum(subsidio_ogu) as subsidio_ogu
    from base
    group by grouping sets (
        (dt_referencia, ano, mes, fonte_familia, prioridade_familia, linha_ogu_fgts),
        (dt_referencia, ano, mes, fonte_familia, prioridade_familia, linha_ogu_fgts, uf)
    )
)

select *
from agg
order by dt_referencia, fonte_familia, nivel_agregacao, uf, linha_ogu_fgts
