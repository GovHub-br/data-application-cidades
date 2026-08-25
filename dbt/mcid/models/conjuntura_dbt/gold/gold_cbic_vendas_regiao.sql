{{ config(materialized='table') }}

-- Mesma lógica de período dinâmico do gold_cbic_lancamentos_regiao.sql —
-- ver comentário lá.

WITH periodos AS (
    SELECT
        ano, trimestre,
        ano * 4 + trimestre AS periodo_idx,
        cbic_vendas_regiao_norte                AS total_norte,
        cbic_vendas_mcmv_regiao_norte           AS mcmv_norte,
        cbic_vendas_mcmv_perc_regiao_norte      AS perc_norte,
        cbic_vendas_regiao_nordeste             AS total_nordeste,
        cbic_vendas_mcmv_regiao_nordeste        AS mcmv_nordeste,
        cbic_vendas_mcmv_perc_regiao_nordeste   AS perc_nordeste,
        cbic_vendas_regiao_centro_oeste             AS total_centro_oeste,
        cbic_vendas_mcmv_regiao_centro_oeste        AS mcmv_centro_oeste,
        cbic_vendas_mcmv_perc_regiao_centro_oeste   AS perc_centro_oeste,
        cbic_vendas_regiao_sudeste              AS total_sudeste,
        cbic_vendas_mcmv_regiao_sudeste         AS mcmv_sudeste,
        cbic_vendas_mcmv_perc_regiao_sudeste    AS perc_sudeste,
        cbic_vendas_regiao_sul                  AS total_sul,
        cbic_vendas_mcmv_regiao_sul             AS mcmv_sul,
        cbic_vendas_mcmv_perc_regiao_sul        AS perc_sul,
        dt_ingest,
        dt_silver
    FROM {{ ref('silver_cbic_lancamentos_vendas') }}
),

regioes AS (
    SELECT 'NORTE' AS regiao, total_norte AS total, mcmv_norte AS mcmv, perc_norte AS perc_mcmv, ano, trimestre, periodo_idx, dt_ingest, dt_silver FROM periodos
    UNION ALL
    SELECT 'NORDESTE', total_nordeste, mcmv_nordeste, perc_nordeste, ano, trimestre, periodo_idx, dt_ingest, dt_silver FROM periodos
    UNION ALL
    SELECT 'CENTRO-OESTE', total_centro_oeste, mcmv_centro_oeste, perc_centro_oeste, ano, trimestre, periodo_idx, dt_ingest, dt_silver FROM periodos
    UNION ALL
    SELECT 'SUDESTE', total_sudeste, mcmv_sudeste, perc_sudeste, ano, trimestre, periodo_idx, dt_ingest, dt_silver FROM periodos
    UNION ALL
    SELECT 'SUL', total_sul, mcmv_sul, perc_sul, ano, trimestre, periodo_idx, dt_ingest, dt_silver FROM periodos
),

referencia AS (
    SELECT MAX(periodo_idx) AS idx_atual FROM regioes
),

periodo_atual AS (
    SELECT regiao, total, mcmv, perc_mcmv, ano, trimestre, dt_ingest, dt_silver
    FROM regioes, referencia WHERE periodo_idx = idx_atual
),

periodo_anterior AS (
    SELECT regiao, perc_mcmv, ano, trimestre FROM regioes, referencia
    WHERE periodo_idx = idx_atual - 1
),

periodo_ano_anterior AS (
    SELECT regiao, perc_mcmv, ano, trimestre FROM regioes, referencia
    WHERE periodo_idx = idx_atual - 4
)

SELECT
    atual.regiao,
    atual.total,
    atual.mcmv,
    atual.trimestre || 'º TRI ' || atual.ano                   AS periodo_atual,
    ROUND(atual.perc_mcmv::numeric, 1)                          AS perc_mcmv_atual,
    ant.trimestre || 'º TRI ' || ant.ano                        AS periodo_tri_anterior,
    ROUND(ant.perc_mcmv::numeric, 1)                            AS perc_mcmv_tri_anterior,
    aa.trimestre || 'º TRI ' || aa.ano                          AS periodo_ano_anterior,
    ROUND(aa.perc_mcmv::numeric, 1)                             AS perc_mcmv_ano_anterior,
    {{ add_metadata_timestamps('gold') }}
FROM periodo_atual atual
LEFT JOIN periodo_anterior ant ON atual.regiao = ant.regiao
LEFT JOIN periodo_ano_anterior aa ON atual.regiao = aa.regiao
ORDER BY
    CASE atual.regiao
        WHEN 'NORTE'        THEN 1
        WHEN 'NORDESTE'     THEN 2
        WHEN 'CENTRO-OESTE' THEN 3
        WHEN 'SUDESTE'      THEN 4
        WHEN 'SUL'          THEN 5
    END
