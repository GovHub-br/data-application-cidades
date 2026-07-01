{{ config(materialized='table') }}

WITH fgts AS (
    SELECT
        ano,
        SUM(financiamento_pf_uh_total_geral)            AS uh_total,
        SUM(
            financiamento_pf_valor_total_faixa_1
            + financiamento_pf_valor_total_faixa_2
            + COALESCE(financiamento_pf_valor_faixa_3_sem_fundo_social, 0)
            + COALESCE(financiamento_pf_valor_faixa_3_fundo_social, 0)
            + COALESCE(financiamento_pf_valor_total_classe_media, 0)
            + COALESCE(financiamento_pf_valor_fora_mcmv, 0)
        )                                               AS val_total,
        SUM(financiamento_pf_uh_pro_cotista_geral)      AS uh_pro_cotista,
        SUM(financiamento_pf_valor_pro_cotista_geral)   AS val_pro_cotista,
        MAX(dt_ingest)                                  AS dt_ingest,
        MAX(dt_silver)                                  AS dt_silver
    FROM {{ ref('silver_fgts_financiamentos_habitacionais') }}
    WHERE ano IN (2024, 2025)
    GROUP BY ano
),

abecip AS (
    SELECT
        ano,
        SUM(sbpe_aquisicao)                             AS sbpe_aq_uh,
        SUM(sbpe_const)                                 AS sbpe_const_uh,
        SUM(sbpe_const_milhoes) / 1000.0                AS sbpe_const_bi,
        SUM(sbpe_aquisicao_milhoes) / 1000.0            AS sbpe_aq_bi,
        SUM(sbpe_total_milhoes) / 1000.0                AS sbpe_total_bi,
        MAX(dt_ingest)                                  AS dt_ingest,
        MAX(dt_silver)                                  AS dt_silver
    FROM {{ ref('silver_abecip_sbpe_financiamentos_habitacionais') }}
    WHERE ano IN (2024, 2025)
    GROUP BY ano
),

f25 AS (
    SELECT ano, uh_total, val_total, uh_pro_cotista, val_pro_cotista, dt_ingest, dt_silver
    FROM fgts WHERE ano = 2025
),
f24 AS (
    SELECT ano, uh_total, val_total, uh_pro_cotista, val_pro_cotista
    FROM fgts WHERE ano = 2024
),
a25 AS (
    SELECT ano, sbpe_aq_uh, sbpe_const_uh, sbpe_const_bi, sbpe_aq_bi, sbpe_total_bi, dt_ingest, dt_silver
    FROM abecip WHERE ano = 2025
),
a24 AS (
    SELECT ano, sbpe_aq_uh, sbpe_const_uh, sbpe_const_bi, sbpe_aq_bi, sbpe_total_bi
    FROM abecip WHERE ano = 2024
),

resultado AS (
    SELECT
        (f25.uh_total + COALESCE(a25.sbpe_aq_uh, 0))                                               AS mcmv_sbpe_uh_25,
        ROUND((f25.val_total::numeric / 1e9) + COALESCE(a25.sbpe_aq_bi, 0), 2)                    AS mcmv_sbpe_val_bi_25,
        ROUND((((f25.uh_total + COALESCE(a25.sbpe_aq_uh, 0))::numeric
            / NULLIF(f24.uh_total + COALESCE(a24.sbpe_aq_uh, 0), 0)) - 1) * 100, 0)               AS mcmv_sbpe_var_uh,
        ROUND((((f25.val_total::numeric / 1e9 + COALESCE(a25.sbpe_aq_bi, 0))
            / NULLIF(f24.val_total::numeric / 1e9 + COALESCE(a24.sbpe_aq_bi, 0), 0)) - 1) * 100, 0) AS mcmv_sbpe_var_val,
        a25.sbpe_const_uh                                                                           AS sbpe_const_uh_25,
        ROUND(a25.sbpe_const_bi::numeric, 2)                                                        AS sbpe_const_bi_25,
        ROUND(((a25.sbpe_const_uh::numeric / NULLIF(a24.sbpe_const_uh, 0)) - 1) * 100, 0)         AS sbpe_const_var_uh,
        ROUND(((a25.sbpe_const_bi / NULLIF(a24.sbpe_const_bi, 0)) - 1) * 100, 0)                  AS sbpe_const_var_bi,
        f25.uh_pro_cotista                                                                          AS pro_cotista_uh_25,
        ROUND(f25.val_pro_cotista::numeric / 1e9, 2)                                               AS pro_cotista_val_bi_25,
        ROUND(((f25.uh_pro_cotista::numeric / NULLIF(f24.uh_pro_cotista, 0)) - 1) * 100, 0)       AS pro_cotista_var_uh,
        ROUND(((f25.val_pro_cotista::numeric / NULLIF(f24.val_pro_cotista, 0)) - 1) * 100, 0)     AS pro_cotista_var_val,
        f25.uh_total                                                                                AS fin_pf_uh_25,
        ROUND(f25.val_total::numeric / 1e9, 2)                                                     AS fin_pf_val_bi_25,
        ROUND(((f25.uh_total::numeric / NULLIF(f24.uh_total, 0)) - 1) * 100, 0)                   AS fin_pf_var_uh,
        ROUND(((f25.val_total::numeric / NULLIF(f24.val_total, 0)) - 1) * 100, 0)                 AS fin_pf_var_val,
        GREATEST(f25.dt_ingest, a25.dt_ingest)                                                     AS dt_ingest,
        GREATEST(f25.dt_silver, a25.dt_silver)                                                     AS dt_silver
    FROM f25, f24, a25, a24
)

SELECT
    mcmv_sbpe_uh_25, mcmv_sbpe_val_bi_25, mcmv_sbpe_var_uh, mcmv_sbpe_var_val,
    sbpe_const_uh_25, sbpe_const_bi_25, sbpe_const_var_uh, sbpe_const_var_bi,
    pro_cotista_uh_25, pro_cotista_val_bi_25, pro_cotista_var_uh, pro_cotista_var_val,
    fin_pf_uh_25, fin_pf_val_bi_25, fin_pf_var_uh, fin_pf_var_val,
    {{ add_metadata_timestamps('gold') }}
FROM resultado