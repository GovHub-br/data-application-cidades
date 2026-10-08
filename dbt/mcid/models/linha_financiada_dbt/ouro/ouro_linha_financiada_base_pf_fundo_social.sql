{{ config(materialized='table') }}

-- Base simples e automatizada que substitui a consolidação manual entre PF
-- FGTS e Fundo Social. O grão é mensal por fonte, faixa, orçamento e imóvel.

with pf_fgts as (
    select
        date_trunc('month', data_contratacao)::date as competencia,
        'FGTS - Base PF'::text as fonte,
        faixa_codigo,
        tipo_orcamento,
        tipo_imovel,
        count(*) as quantidade_contratos,
        sum(valor_financiamento) as valor_financiamento
    from {{ ref('prata_linha_financiada_pf_fgts') }}
    group by 1, 2, 3, 4, 5
),

fundo_social as (
    select
        date_trunc('month', data_contratacao)::date as competencia,
        'Fundo Social'::text as fonte,
        faixa_codigo,
        'Fundo Social'::text as tipo_orcamento,
        tipo_imovel,
        count(*) as quantidade_contratos,
        sum(valor_financiamento) as valor_financiamento
    from {{ ref('prata_linha_financiada_contrato') }}
    where ic_fundo_social
    group by 1, 2, 3, 4, 5
)

select *, current_timestamp as dt_gold from pf_fgts
union all
select *, current_timestamp as dt_gold from fundo_social
