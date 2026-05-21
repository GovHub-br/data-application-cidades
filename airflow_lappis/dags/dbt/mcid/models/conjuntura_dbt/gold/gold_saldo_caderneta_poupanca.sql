{{ config(materialized='table') }}

WITH base AS (
    SELECT
        data_referencia,
        captacao_liquida_valor,
        ROUND(captacao_liquida_valor::numeric / 1e3, 1) AS cap_liq_bi,
        dt_ingest,
        dt_silver
    FROM {{ ref('silver_abecip_poupanca_sbpe') }}
),

dez25 AS (SELECT * FROM base WHERE data_referencia = '2025-12-01'),
nov25 AS (SELECT * FROM base WHERE data_referencia = '2025-11-01'),
dez24 AS (SELECT * FROM base WHERE data_referencia = '2024-12-01'),

acum_dez25 AS (
    SELECT
        ROUND(SUM(captacao_liquida_valor::numeric / 1e3), 1) AS total,
        MAX(dt_ingest)                                       AS dt_ingest,
        MAX(dt_silver)                                       AS dt_silver
    FROM base
    WHERE data_referencia BETWEEN '2025-01-01' AND '2025-12-01'
),

acum_dez24 AS (
    SELECT
        ROUND(SUM(captacao_liquida_valor::numeric / 1e3), 1) AS total,
        MAX(dt_ingest)                                       AS dt_ingest,
        MAX(dt_silver)                                       AS dt_silver
    FROM base
    WHERE data_referencia BETWEEN '2024-01-01' AND '2024-12-01'
),

resultado AS (
    SELECT 1 AS ordem, 'DEZ 2025'         AS periodo, d25.cap_liq_bi AS cap_liq_bi, d25.dt_ingest, d25.dt_silver FROM dez25 d25
    UNION ALL SELECT 2, 'NOV 2025',        n25.cap_liq_bi, n25.dt_ingest, n25.dt_silver FROM nov25 n25
    UNION ALL SELECT 3, 'DEZ 2024',        d24.cap_liq_bi, d24.dt_ingest, d24.dt_silver FROM dez24 d24
    UNION ALL SELECT 4, '12 MESES – DEZ/25', a25.total,   a25.dt_ingest, a25.dt_silver FROM acum_dez25 a25
    UNION ALL SELECT 5, '12 MESES – DEZ/24', a24.total,   a24.dt_ingest, a24.dt_silver FROM acum_dez24 a24
)

SELECT
    ordem,
    periodo,
    cap_liq_bi,
    {{ add_metadata_timestamps('gold') }}
FROM resultado
ORDER BY ordem