{{ config(materialized='table') }}

WITH base AS (
    SELECT
        ano,
        SUM(financiamento_pf_uh_total_faixa_1)               AS f1_uh,
        SUM(financiamento_pf_valor_total_faixa_1)             AS f1_val,
        SUM(financiamento_pf_uh_total_faixa_2)               AS f2_uh,
        SUM(financiamento_pf_valor_total_faixa_2)             AS f2_val,
        SUM(financiamento_pf_uh_faixa_3_sem_fundo_social)    AS f3_uh,
        SUM(financiamento_pf_valor_faixa_3_sem_fundo_social)  AS f3_val,
        SUM(financiamento_pf_uh_pro_cotista_geral)            AS pc_uh,
        SUM(financiamento_pf_valor_pro_cotista_geral)         AS pc_val,
        SUM(financiamento_pf_uh_pro_cotista_faixa_1)          AS pc_f1_uh,
        SUM(financiamento_pf_valor_pro_cotista_faixa_1)       AS pc_f1_val,
        SUM(financiamento_pf_uh_pro_cotista_faixa_2)          AS pc_f2_uh,
        SUM(financiamento_pf_valor_pro_cotista_faixa_2)       AS pc_f2_val,
        SUM(financiamento_pf_uh_pro_cotista_faixa_3)          AS pc_f3_uh,
        SUM(financiamento_pf_valor_pro_cotista_faixa_3)       AS pc_f3_val,
        SUM(financiamento_pf_uh_pro_cotista_classe_media)     AS pc_cm_uh,
        SUM(financiamento_pf_valor_pro_cotista_classe_media)  AS pc_cm_val,
        SUM(financiamento_pf_uh_fora_mcmv)                    AS fora_uh,
        SUM(financiamento_pf_valor_fora_mcmv)                 AS fora_val,
        SUM(financiamento_pf_uh_faixa_3_fundo_social)         AS f3fs_uh,
        SUM(financiamento_pf_valor_faixa_3_fundo_social)      AS f3fs_val,
        SUM(financiamento_pf_uh_total_classe_media)           AS cm_uh,
        SUM(financiamento_pf_valor_total_classe_media)        AS cm_val,
        SUM(financiamento_pf_uh_total_geral)                  AS total_uh,
        SUM(valor_total_calculado)                            AS total_val
    FROM {{ ref('silver_fgts_financiamentos_habitacionais') }}
    WHERE ano IN (2024, 2025)
    GROUP BY ano
),

y24 AS (SELECT * FROM base WHERE ano = 2024),
y25 AS (SELECT * FROM base WHERE ano = 2025)

SELECT 1 AS ordem, 'Faixa 1' AS categoria,
    a.f1_uh AS uh_2024, ROUND(a.f1_val::numeric / 1e9, 2) AS val_bi_2024,
    b.f1_uh AS uh_2025, ROUND(b.f1_val::numeric / 1e9, 2) AS val_bi_2025
FROM y24 a, y25 b
UNION ALL SELECT 2, 'Faixa 2',
    a.f2_uh, ROUND(a.f2_val::numeric / 1e9, 2), b.f2_uh, ROUND(b.f2_val::numeric / 1e9, 2)
FROM y24 a, y25 b
UNION ALL SELECT 3, 'Faixa 3',
    a.f3_uh, ROUND(a.f3_val::numeric / 1e9, 2), b.f3_uh, ROUND(b.f3_val::numeric / 1e9, 2)
FROM y24 a, y25 b
UNION ALL SELECT 4, 'Pró-Cotista',
    a.pc_uh, ROUND(a.pc_val::numeric / 1e9, 2), b.pc_uh, ROUND(b.pc_val::numeric / 1e9, 2)
FROM y24 a, y25 b
UNION ALL SELECT 5, '  Faixa 1',
    a.pc_f1_uh, ROUND(a.pc_f1_val::numeric / 1e9, 3), b.pc_f1_uh, ROUND(b.pc_f1_val::numeric / 1e9, 3)
FROM y24 a, y25 b
UNION ALL SELECT 6, '  Faixa 2',
    a.pc_f2_uh, ROUND(a.pc_f2_val::numeric / 1e9, 3), b.pc_f2_uh, ROUND(b.pc_f2_val::numeric / 1e9, 3)
FROM y24 a, y25 b
UNION ALL SELECT 7, '  Faixa 3',
    a.pc_f3_uh, ROUND(a.pc_f3_val::numeric / 1e9, 3), b.pc_f3_uh, ROUND(b.pc_f3_val::numeric / 1e9, 3)
FROM y24 a, y25 b
UNION ALL SELECT 8, '  Faixa Classe Média',
    a.pc_cm_uh, ROUND(a.pc_cm_val::numeric / 1e9, 3), b.pc_cm_uh, ROUND(b.pc_cm_val::numeric / 1e9, 3)
FROM y24 a, y25 b
UNION ALL SELECT 9, '  Fora MCMV',
    a.fora_uh, ROUND(a.fora_val::numeric / 1e9, 2), b.fora_uh, ROUND(b.fora_val::numeric / 1e9, 2)
FROM y24 a, y25 b
UNION ALL SELECT 10, 'Faixa 3 Fundo Social',
    a.f3fs_uh, ROUND(a.f3fs_val::numeric / 1e9, 2), b.f3fs_uh, ROUND(b.f3fs_val::numeric / 1e9, 2)
FROM y24 a, y25 b
UNION ALL SELECT 11, 'Faixa Classe Média',
    a.cm_uh, ROUND(a.cm_val::numeric / 1e9, 2), b.cm_uh, ROUND(b.cm_val::numeric / 1e9, 2)
FROM y24 a, y25 b
UNION ALL SELECT 12, 'TOTAL',
    a.total_uh, ROUND(a.total_val::numeric / 1e9, 2), b.total_uh, ROUND(b.total_val::numeric / 1e9, 2)
FROM y24 a, y25 b
ORDER BY ordem