{{ config(materialized="table") }}

-- PRATA cross-frente do reloginho — resumo mensal de contratos e valor para
-- Classe Média, Reforma Casa Brasil, MCMV Cidades e Pró-Moradia (change
-- integrar-frentes-novas-reloginho, D1/D2/D3 do design.md).
--
-- GRÃO POR dt_contratacao, NUNCA POR dt_referencia (D2): as pratas de origem
-- em mcmv_historico_dbt gravam dt_referencia como a data do SNAPSHOT semanal
-- de coleta — o mesmo contrato reaparece em dezenas de snapshots sucessivos
-- (achado com dado real: Classe Média 137.492 contratos em 3.407.434 linhas,
-- razão ~24,8×; Reforma Casa Brasil 136.078 em 2.713.431, ~19,9×; MCMV
-- Cidades ~1,2×; Pró-Moradia já é grão de contrato, 1×). Agrupar por
-- dt_referencia multiplicaria n_contratos/valor_contratado pela razão
-- linhas/contrato de cada frente. Por isso cada CTE abaixo primeiro
-- deduplica para 1 linha por contrato (mantendo a observação de
-- dt_referencia mais recente via row_number()) e só então deriva
-- mes_referencia a partir de dt_contratacao (data do evento, 100% preenchida
-- nas 4 fontes — conferido nesta change). NÃO reintroduzir agregação por
-- dt_referencia sem reler o design.md (D2) — é o mesmo bug que este modelo
-- existe para evitar.
--
-- MCMV Cidades particiona a dedup também por fonte_bronze (D3, herdado de
-- prata_hist_mcmv_cidades_contrato) — GEFUS e SharePoint/emendas nunca
-- são casadas linha a linha entre si.
--
-- FNHIS/SUB50 fica de fora (D4): dt_contratacao é NULL para
-- status_proposta = 'apresentada' (maioria das linhas) e há só 1
-- dt_referencia no dataset — não há série mensal real para agregar hoje.
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
            row_number() over (
                partition by nu_contrato order by dt_referencia desc
            ) as rn
        from {{ classe_media }}
    ),

    classe_media_dedup as (
        select
            frente_mcmv,
            date_trunc('month', dt_contratacao)::date as mes_referencia,
            valor_contratado
        from classe_media_rn
        where rn = 1
    ),

    reforma_rn as (
        select
            frente_mcmv,
            dt_contratacao,
            valor_contratado,
            row_number() over (
                partition by nu_contrato order by dt_referencia desc
            ) as rn
        from {{ reforma }}
    ),

    reforma_dedup as (
        select
            frente_mcmv,
            date_trunc('month', dt_contratacao)::date as mes_referencia,
            valor_contratado
        from reforma_rn
        where rn = 1
    ),

    mcmv_cidades_rn as (
        select
            frente_mcmv,
            dt_contratacao,
            valor_contratado,
            row_number() over (
                partition by fonte_bronze, nu_contrato order by dt_referencia desc
            ) as rn
        from {{ mcmv_cidades }}
    ),

    mcmv_cidades_dedup as (
        select
            frente_mcmv,
            date_trunc('month', dt_contratacao)::date as mes_referencia,
            valor_contratado
        from mcmv_cidades_rn
        where rn = 1
    ),

    pro_moradia_rn as (
        select
            frente_mcmv,
            dt_contratacao,
            valor_contratado,
            row_number() over (
                partition by codigo_contrato order by dt_referencia desc
            ) as rn
        from {{ pro_moradia }}
    ),

    pro_moradia_dedup as (
        select
            frente_mcmv,
            date_trunc('month', dt_contratacao)::date as mes_referencia,
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
    count(*) as n_contratos,
    sum(valor_contratado) as valor_contratado
from unioned
-- Contratos sem dt_contratacao não entram na série mensal (D2/task 3.3) —
-- cobertura resultante documentada no schema.yml deste modelo, não
-- descartada silenciosamente.
where mes_referencia is not null
group by frente_mcmv, mes_referencia
