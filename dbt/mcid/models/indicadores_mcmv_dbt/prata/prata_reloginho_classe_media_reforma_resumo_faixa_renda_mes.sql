{{ config(materialized="table") }}

-- PRATA cross-frente do reloginho — resumo mensal por faixa de renda
-- (contratos + valor), restrita a Classe Média e Reforma Casa Brasil: as
-- únicas 2 das 4 frentes novas com `faixa_renda` no contrato de origem
-- (change criar-ouro-reloginho-frentes-novas, design.md D1). MCMV Cidades e
-- Pró-Moradia não aparecem aqui — não têm essa coluna, e ausência de
-- informação não é o mesmo que "faixa nula" (design.md D1, alternativa
-- descartada de um único modelo com GROUPING SETS cobrindo as 4 frentes).
--
-- Mesma dedup por `nu_contrato` das demais pratas cross-frente (row_number()
-- por dt_referencia desc, 1 linha por contrato antes de agregar por mês).
{% set classe_media = ref('prata_classe_media_historico_contrato') %}
{% set reforma = ref('prata_reforma_casa_brasil_historico_contrato') %}

with

    classe_media_rn as (
        select
            frente_mcmv,
            dt_contratacao,
            valor_contratado,
            faixa_renda,
            row_number() over (
                partition by nu_contrato order by dt_referencia desc
            ) as rn
        from {{ classe_media }}
    ),

    classe_media_dedup as (
        select
            frente_mcmv,
            date_trunc('month', dt_contratacao)::date as mes_referencia,
            faixa_renda,
            valor_contratado
        from classe_media_rn
        where rn = 1
    ),

    reforma_rn as (
        select
            frente_mcmv,
            dt_contratacao,
            valor_contratado,
            faixa_renda,
            row_number() over (
                partition by nu_contrato order by dt_referencia desc
            ) as rn
        from {{ reforma }}
    ),

    reforma_dedup as (
        select
            frente_mcmv,
            date_trunc('month', dt_contratacao)::date as mes_referencia,
            faixa_renda,
            valor_contratado
        from reforma_rn
        where rn = 1
    ),

    unioned as (
        select * from classe_media_dedup
        union all
        select * from reforma_dedup
    )

select
    frente_mcmv,
    mes_referencia,
    faixa_renda,
    count(*) as n_contratos,
    sum(valor_contratado) as valor_contratado
from unioned
-- Contratos sem dt_contratacao não entram na série mensal, mesma decisão de
-- prata_reloginho_frentes_novas_resumo_mes.
where mes_referencia is not null
group by frente_mcmv, mes_referencia, faixa_renda
