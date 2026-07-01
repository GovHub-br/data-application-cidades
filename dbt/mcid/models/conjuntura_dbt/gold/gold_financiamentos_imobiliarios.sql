{{ config(materialized='table') }}

WITH pivotado AS (
    SELECT
        data_referencia,
        MAX(CASE WHEN tipo = 'pf_concessoes_rs_mi'  THEN valor END) AS pf_concessoes,
        MAX(CASE WHEN tipo = 'pf_taxa_juros_aa'      THEN valor END) AS pf_taxa_juros,
        MAX(CASE WHEN tipo = 'pf_inadimplencia_pct'  THEN valor END) AS pf_inadimplencia,
        MAX(CASE WHEN tipo = 'pj_concessoes_rs_mi'   THEN valor END) AS pj_concessoes,
        MAX(CASE WHEN tipo = 'pj_taxa_juros_aa'      THEN valor END) AS pj_taxa_juros,
        MAX(CASE WHEN tipo = 'pj_inadimplencia_pct'  THEN valor END) AS pj_inadimplencia,
        MAX(dt_ingest)                                               AS dt_ingest,
        MAX(dt_silver)                                               AS dt_silver
    FROM {{ ref('silver_bacen_financiamentos_imobiliarios') }}
    GROUP BY data_referencia
),

dez25 AS (
    SELECT pf_concessoes, pf_taxa_juros, pf_inadimplencia, pj_concessoes, pj_taxa_juros, pj_inadimplencia, dt_ingest, dt_silver
    FROM pivotado WHERE data_referencia = '202501-12-01'
),

nov25 AS (
    SELECT pf_concessoes, pf_taxa_juros, pf_inadimplencia, pj_concessoes, pj_taxa_juros, pj_inadimplencia, dt_ingest, dt_silver
    FROM pivotado WHERE data_referencia = '202501-11-01'
),

dez24 AS (
    SELECT pf_concessoes, pf_taxa_juros, pf_inadimplencia, pj_concessoes, pj_taxa_juros, pj_inadimplencia, dt_ingest, dt_silver
    FROM pivotado WHERE data_referencia = '202401-12-01'
),

acum_dez25 AS (
    SELECT
        SUM(pf_concessoes)  AS pf_concessoes,
        SUM(pj_concessoes)  AS pj_concessoes,
        MAX(dt_ingest)      AS dt_ingest,
        MAX(dt_silver)      AS dt_silver
    FROM pivotado
    WHERE data_referencia BETWEEN '202501-01-01' AND '202501-12-01'
),

acum_dez24 AS (
    SELECT
        SUM(pf_concessoes)  AS pf_concessoes,
        SUM(pj_concessoes)  AS pj_concessoes,
        MAX(dt_ingest)      AS dt_ingest,
        MAX(dt_silver)      AS dt_silver
    FROM pivotado
    WHERE data_referencia BETWEEN '202401-01-01' AND '202401-12-01'
),

resultado AS (
    SELECT 'dez/25' AS periodo,
        d25.pf_concessoes, d25.pf_taxa_juros, d25.pf_inadimplencia,
        d25.pj_concessoes, d25.pj_taxa_juros, d25.pj_inadimplencia,
        d25.dt_ingest, d25.dt_silver
    FROM dez25 d25
    UNION ALL
    SELECT 'nov/25',
        n25.pf_concessoes, n25.pf_taxa_juros, n25.pf_inadimplencia,
        n25.pj_concessoes, n25.pj_taxa_juros, n25.pj_inadimplencia,
        n25.dt_ingest, n25.dt_silver
    FROM nov25 n25
    UNION ALL
    SELECT 'dez/24',
        d24.pf_concessoes, d24.pf_taxa_juros, d24.pf_inadimplencia,
        d24.pj_concessoes, d24.pj_taxa_juros, d24.pj_inadimplencia,
        d24.dt_ingest, d24.dt_silver
    FROM dez24 d24
    UNION ALL
    SELECT '12 meses - dez/25',
        a25.pf_concessoes, NULL, NULL,
        a25.pj_concessoes, NULL, NULL,
        a25.dt_ingest, a25.dt_silver
    FROM acum_dez25 a25
    UNION ALL
    SELECT '12 meses - dez/24',
        a24.pf_concessoes, NULL, NULL,
        a24.pj_concessoes, NULL, NULL,
        a24.dt_ingest, a24.dt_silver
    FROM acum_dez24 a24
)

SELECT
    periodo,
    pf_concessoes,
    pf_taxa_juros,
    pf_inadimplencia,
    pj_concessoes,
    pj_taxa_juros,
    pj_inadimplencia,
    {{ add_metadata_timestamps('gold') }}
FROM resultado