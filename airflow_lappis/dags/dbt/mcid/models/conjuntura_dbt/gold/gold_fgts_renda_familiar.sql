{{ config(materialized='table') }}

WITH base AS (
    SELECT
        ano,
        -- FAIXA 1
        SUM(financiamento_pf_uh_pro_cotista_faixa_1)                                                           AS f1_usadas,
        SUM(financiamento_pf_uh_total_faixa_1 - COALESCE(financiamento_pf_uh_pro_cotista_faixa_1, 0))          AS f1_novas,
        -- FAIXA 2
        SUM(financiamento_pf_uh_pro_cotista_faixa_2)                                                           AS f2_usadas,
        SUM(financiamento_pf_uh_total_faixa_2 - COALESCE(financiamento_pf_uh_pro_cotista_faixa_2, 0))          AS f2_novas,
        -- FAIXA 3 sem fundo social
        SUM(financiamento_pf_uh_pro_cotista_faixa_3)                                                           AS f3_usadas,
        SUM(financiamento_pf_uh_faixa_3_sem_fundo_social - COALESCE(financiamento_pf_uh_pro_cotista_faixa_3, 0)) AS f3_novas,
        -- FAIXA 3 FS
        SUM(financiamento_pf_uh_faixa_3_fundo_social)                                                          AS f3fs_novas,
        -- FAIXA CLASSE MÉDIA
        SUM(financiamento_pf_uh_pro_cotista_classe_media)                                                      AS cm_usadas,
        SUM(financiamento_pf_uh_total_classe_media - COALESCE(financiamento_pf_uh_pro_cotista_classe_media, 0)) AS cm_novas,
        -- FORA MCMV
        SUM(financiamento_pf_uh_fora_mcmv)                                                                     AS fora_usadas,
        -- TOTAIS
        SUM(financiamento_pf_uh_pro_cotista_geral)                                                             AS total_usadas,
        SUM(financiamento_pf_uh_total_geral - COALESCE(financiamento_pf_uh_pro_cotista_geral, 0))              AS total_novas
    FROM {{ ref('silver_fgts_financiamentos_habitacionais') }}
    WHERE ano IN (2024, 2025)
    GROUP BY ano
),

y24 AS (SELECT * FROM base WHERE ano = 2024),
y25 AS (SELECT * FROM base WHERE ano = 2025)

SELECT 1 AS ordem, 'FAIXA 1' AS categoria,
    a.f1_usadas AS uh_usadas_2024, a.f1_novas AS uh_novas_2024,
    b.f1_usadas AS uh_usadas_2025,
    ROUND(((b.f1_usadas::numeric / NULLIF(a.f1_usadas, 0)) - 1) * 100, 0) AS var_usadas,
    b.f1_novas  AS uh_novas_2025,
    ROUND(((b.f1_novas::numeric  / NULLIF(a.f1_novas,  0)) - 1) * 100, 0) AS var_novas
FROM y24 a, y25 b

UNION ALL SELECT 2, 'FAIXA 2',
    a.f2_usadas, a.f2_novas,
    b.f2_usadas, ROUND(((b.f2_usadas::numeric / NULLIF(a.f2_usadas, 0)) - 1) * 100, 0),
    b.f2_novas,  ROUND(((b.f2_novas::numeric  / NULLIF(a.f2_novas,  0)) - 1) * 100, 0)
FROM y24 a, y25 b

UNION ALL SELECT 3, 'FAIXA 3',
    a.f3_usadas, a.f3_novas,
    b.f3_usadas, ROUND(((b.f3_usadas::numeric / NULLIF(a.f3_usadas, 0)) - 1) * 100, 0),
    b.f3_novas,  ROUND(((b.f3_novas::numeric  / NULLIF(a.f3_novas,  0)) - 1) * 100, 0)
FROM y24 a, y25 b

UNION ALL SELECT 4, 'FAIXA 3 FS',
    NULL, NULL, NULL, NULL,
    b.f3fs_novas, ROUND(((b.f3fs_novas::numeric / NULLIF(a.f3fs_novas, 0)) - 1) * 100, 0)
FROM y24 a, y25 b

UNION ALL SELECT 5, 'FAIXA CLASSE MÉDIA',
    NULL, NULL,
    b.cm_usadas, ROUND(((b.cm_usadas::numeric / NULLIF(a.cm_usadas, 0)) - 1) * 100, 0),
    b.cm_novas,  ROUND(((b.cm_novas::numeric  / NULLIF(a.cm_novas,  0)) - 1) * 100, 0)
FROM y24 a, y25 b

UNION ALL SELECT 6, 'FORA MCMV',
    a.fora_usadas, NULL,
    b.fora_usadas, ROUND(((b.fora_usadas::numeric / NULLIF(a.fora_usadas, 0)) - 1) * 100, 0),
    NULL, NULL
FROM y24 a, y25 b

UNION ALL SELECT 7, 'TOTAL',
    a.total_usadas, a.total_novas,
    b.total_usadas, ROUND(((b.total_usadas::numeric / NULLIF(a.total_usadas, 0)) - 1) * 100, 0),
    b.total_novas,  ROUND(((b.total_novas::numeric  / NULLIF(a.total_novas,  0)) - 1) * 100, 0)
FROM y24 a, y25 b

ORDER BY ordem