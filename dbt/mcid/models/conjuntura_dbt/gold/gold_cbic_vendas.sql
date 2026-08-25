{{ config(materialized='table') }}

-- Mesma lógica de período dinâmico do gold_cbic_lancamentos.sql — ver
-- comentário lá.

WITH base AS (
    SELECT
        ano,
        trimestre,
        ano * 4 + trimestre AS periodo_idx,
        cbic_vendas_total    AS total,
        cbic_vendas_mcmv     AS mcmv,
        cbic_vendas_demais   AS demais,
        dt_ingest,
        dt_silver
    FROM {{ ref('silver_cbic_lancamentos_vendas') }}
),

referencia AS (
    SELECT ano AS ano_atual, trimestre AS trimestre_atual, periodo_idx AS idx_atual
    FROM base ORDER BY periodo_idx DESC LIMIT 1
),

periodos AS (
    SELECT 1 AS ordem, trimestre || 'º TRI ' || ano AS periodo, total, mcmv, demais, dt_ingest, dt_silver
    FROM base, referencia WHERE periodo_idx = idx_atual
    UNION ALL
    SELECT 2, trimestre || 'º TRI ' || ano, total, mcmv, demais, dt_ingest, dt_silver
    FROM base, referencia WHERE periodo_idx = idx_atual - 1
    UNION ALL
    SELECT 3, trimestre || 'º TRI ' || ano, total, mcmv, demais, dt_ingest, dt_silver
    FROM base, referencia WHERE periodo_idx = idx_atual - 4
    UNION ALL
    SELECT
        4,
        '12 MESES - ' || CASE trimestre_atual WHEN 1 THEN 'MAR' WHEN 2 THEN 'JUN' WHEN 3 THEN 'SET' ELSE 'DEZ' END || '/' || ano_atual,
        SUM(total), SUM(mcmv), SUM(demais), MAX(dt_ingest), MAX(dt_silver)
    FROM base, referencia WHERE periodo_idx BETWEEN idx_atual - 3 AND idx_atual
    GROUP BY trimestre_atual, ano_atual
    UNION ALL
    SELECT
        5,
        '12 MESES - ' || CASE trimestre_atual WHEN 1 THEN 'MAR' WHEN 2 THEN 'JUN' WHEN 3 THEN 'SET' ELSE 'DEZ' END || '/' || (ano_atual - 1),
        SUM(total), SUM(mcmv), SUM(demais), MAX(dt_ingest), MAX(dt_silver)
    FROM base, referencia WHERE periodo_idx BETWEEN idx_atual - 7 AND idx_atual - 4
    GROUP BY trimestre_atual, ano_atual
)

SELECT
    periodo,
    total,
    mcmv,
    demais,
    {{ add_metadata_timestamps('gold') }}
FROM periodos
ORDER BY ordem
