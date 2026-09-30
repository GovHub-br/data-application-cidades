{{ config(materialized="table") }}

-- PRATA cross-frente do reloginho — resumo mensal por UF (contratos + valor)
-- de Classe Média, Reforma Casa Brasil, MCMV Cidades e Pró-Moradia (change
-- criar-ouro-reloginho-frentes-novas, design.md D1).
--
-- Mesma disciplina de dedup-por-chave-de-negócio-antes-de-agregar de
-- `prata_relog_frentes_novas_resumo_mes` (mesmas 4 CTEs, mesmas chaves:
-- `nu_contrato` / `(fonte_bronze, nu_contrato)` / `codigo_contrato`) — só
-- adiciona `uf` na projeção e no agrupamento. NÃO estender
-- `prata_relog_frentes_novas_resumo_mes` com GROUPING SETS em vez deste
-- modelo dedicado — mudaria o grão dela e quebraria o teste
-- `unique_combinacao` já declarado (design.md D1).
--
-- FNHIS/SUB50 fica de fora, mesma razão da prata sem UF: sem dt_contratacao
-- confiável para compor série mensal real.
{% set classe_media = ref('prata_hist_classe_media_contrato') %}
{% set reforma = ref('prata_hist_reforma_casa_brasil_contrato') %}
{% set mcmv_cidades = ref('prata_hist_mcmv_cidades_contrato') %}
{% set pro_moradia = ref('prata_hist_pro_moradia_contrato') %}

with

    classe_media_rn as (
        select
            frente_mcmv,
            dt_contratacao,
            valor_contratado,
            uf,
            row_number() over (
                partition by nu_contrato order by dt_referencia desc
            ) as rn
        from {{ classe_media }}
    ),

    classe_media_dedup as (
        select
            frente_mcmv,
            date_trunc('month', dt_contratacao)::date as mes_referencia,
            upper(trim(uf)) as uf,
            valor_contratado
        from classe_media_rn
        where rn = 1
    ),

    reforma_rn as (
        select
            frente_mcmv,
            dt_contratacao,
            valor_contratado,
            uf,
            row_number() over (
                partition by nu_contrato order by dt_referencia desc
            ) as rn
        from {{ reforma }}
    ),

    reforma_dedup as (
        select
            frente_mcmv,
            date_trunc('month', dt_contratacao)::date as mes_referencia,
            upper(trim(uf)) as uf,
            valor_contratado
        from reforma_rn
        where rn = 1
    ),

    mcmv_cidades_rn as (
        select
            frente_mcmv,
            dt_contratacao,
            valor_contratado,
            uf,
            row_number() over (
                partition by fonte_bronze, nu_contrato order by dt_referencia desc
            ) as rn
        from {{ mcmv_cidades }}
    ),

    mcmv_cidades_dedup as (
        select
            frente_mcmv,
            date_trunc('month', dt_contratacao)::date as mes_referencia,
            upper(trim(uf)) as uf,
            valor_contratado
        from mcmv_cidades_rn
        where rn = 1
    ),

    pro_moradia_rn as (
        select
            frente_mcmv,
            dt_contratacao,
            valor_contratado,
            uf,
            row_number() over (
                partition by codigo_contrato order by dt_referencia desc
            ) as rn
        from {{ pro_moradia }}
    ),

    pro_moradia_dedup as (
        select
            frente_mcmv,
            date_trunc('month', dt_contratacao)::date as mes_referencia,
            upper(trim(uf)) as uf,
            valor_contratado
        from pro_moradia_rn
        where rn = 1
    ),

    unioned as (
        select * from classe_media_dedup
        union all
        select * from reforma_dedup
        union all
        select * from mcmv_cidades_dedup
        union all
        select * from pro_moradia_dedup
    )

select
    frente_mcmv,
    mes_referencia,
    uf,
    count(*) as n_contratos,
    sum(valor_contratado) as valor_contratado
from unioned
-- Contratos sem dt_contratacao não entram na série mensal, mesma decisão de
-- prata_relog_frentes_novas_resumo_mes.
where mes_referencia is not null
group by frente_mcmv, mes_referencia, uf
