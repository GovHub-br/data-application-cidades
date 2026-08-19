{{ config(materialized='table') }}

WITH fgts AS (
    SELECT
        ano,
        SUM(financiamento_pf_uh_pro_cotista_geral)                                              AS uh_usadas,
        SUM(financiamento_pf_uh_total_geral - COALESCE(financiamento_pf_uh_pro_cotista_geral, 0)) AS uh_novas,
        MAX(dt_ingest)                                                                           AS dt_ingest,
        MAX(dt_silver)                                                                           AS dt_silver
    FROM {{ ref('silver_fgts_financiamentos_habitacionais') }}
    WHERE ano IN (2024, 2025)
    GROUP BY ano
),

abecip AS (
    SELECT
        ano,
        SUM(sbpe_aq_usados_uh)  AS uh_usadas,
        SUM(sbpe_aq_novos_uh)   AS uh_novas,
        MAX(dt_ingest)          AS dt_ingest,
        MAX(dt_silver)          AS dt_silver
    FROM {{ ref('silver_abecip_sbpe_financiamentos_habitacionais') }}
    WHERE ano IN (2024, 2025)
    GROUP BY ano
),

f24 AS (SELECT * FROM fgts  WHERE ano = 2024),
f25 AS (SELECT * FROM fgts  WHERE ano = 2025),
a24 AS (SELECT * FROM abecip WHERE ano = 2024),
a25 AS (SELECT * FROM abecip WHERE ano = 2025),

resultado AS (
    SELECT 1 AS ordem, 'FGTS - PF' AS categoria,
        f24.uh_usadas AS uh_usadas_2024, f24.uh_novas AS uh_novas_2024,
        f25.uh_usadas AS uh_usadas_2025,
        ROUND(((f25.uh_usadas::numeric / NULLIF(f24.uh_usadas, 0)) - 1) * 100, 0) AS var_usadas,
        f25.uh_novas  AS uh_novas_2025,
        ROUND(((f25.uh_novas::numeric  / NULLIF(f24.uh_novas,  0)) - 1) * 100, 0) AS var_novas,
        GREATEST(f25.dt_ingest, f24.dt_ingest)  AS dt_ingest,
        GREATEST(f25.dt_silver, f24.dt_silver)  AS dt_silver
    FROM f24, f25
    UNION ALL
    SELECT 2, 'SBPE (Aquisição)',
        a24.uh_usadas, a24.uh_novas,
        a25.uh_usadas, ROUND(((a25.uh_usadas::numeric / NULLIF(a24.uh_usadas, 0)) - 1) * 100, 0),
        a25.uh_novas,  ROUND(((a25.uh_novas::numeric  / NULLIF(a24.uh_novas,  0)) - 1) * 100, 0),
        GREATEST(a25.dt_ingest, a24.dt_ingest),
        GREATEST(a25.dt_silver, a24.dt_silver)
    FROM a24, a25
    UNION ALL
    SELECT 3, 'Total',
        (f24.uh_usadas + a24.uh_usadas),
        (f24.uh_novas  + a24.uh_novas),
        (f25.uh_usadas + a25.uh_usadas),
        ROUND((((f25.uh_usadas + a25.uh_usadas)::numeric / NULLIF(f24.uh_usadas + a24.uh_usadas, 0)) - 1) * 100, 0),
        (f25.uh_novas  + a25.uh_novas),
        ROUND((((f25.uh_novas  + a25.uh_novas)::numeric  / NULLIF(f24.uh_novas  + a24.uh_novas,  0)) - 1) * 100, 0),
        GREATEST(f25.dt_ingest, a25.dt_ingest),
        GREATEST(f25.dt_silver, a25.dt_silver)
    FROM f24, f25, a24, a25
)

SELECT
    ordem,
    categoria,
    uh_usadas_2024,
    uh_novas_2024,
    uh_usadas_2025,
    var_usadas,
    uh_novas_2025,
    var_novas,
    {{ add_metadata_timestamps('gold') }}
FROM resultado
ORDER BY ordem