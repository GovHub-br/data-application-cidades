{{ config(materialized='table') }}

-- Período mais recente é sempre o de maior (ano, trimestre) disponível na
-- silver — antes vinha hardcoded (ex.: "WHERE ano = 2025 AND trimestre = 4"),
-- então dado novo inserido manualmente (1T2026, 2T2026...) não aparecia no
-- boletim sozinho. "12 meses" agora é uma janela móvel dos últimos 4
-- trimestres a partir do mais recente, não mais o ano-calendário fechado
-- (que ficaria errado pra um ano ainda incompleto, tipo 2026 com só 2
-- trimestres de dado).

WITH base AS (
    SELECT
        ano,
        trimestre,
        ano * 4 + trimestre     AS periodo_idx,
        cbic_lancamentos_total  AS total,
        cbic_lancamentos_mcmv   AS mcmv,
        cbic_lancamentos_demais AS demais,
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
