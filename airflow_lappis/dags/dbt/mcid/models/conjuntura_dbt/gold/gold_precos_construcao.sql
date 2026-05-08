{{ config(materialized='table') }}

WITH sinapi_dez25 AS (
    SELECT * FROM {{ ref('silver_ibge_sinapi') }} WHERE data_referencia = '2025-12-01'
),
sinapi_dez24 AS (
    SELECT * FROM {{ ref('silver_ibge_sinapi') }} WHERE data_referencia = '2024-12-01'
),
incc_dez25 AS (
    SELECT * FROM {{ ref('silver_fgv_incc_m') }} WHERE data_referencia = '2025-12-01'
),
incc_dez24 AS (
    SELECT * FROM {{ ref('silver_fgv_incc_m') }} WHERE data_referencia = '2024-12-01'
)

SELECT
    -- SINAPI
    ROUND(s25.custo_m2::numeric, 1)     AS sinapi_custo_m2_dez25,
    ROUND(s25.var_mes::numeric, 2)      AS sinapi_var_mes_dez25,
    ROUND(s25.var_12m::numeric, 2)      AS sinapi_var_12m_dez25,
    ROUND(s24.var_12m::numeric, 2)      AS sinapi_var_12m_dez24,
    -- INCC-M
    ROUND(i25.indice::numeric, 1)       AS incc_indice_dez25,
    ROUND(i25.var_mes::numeric, 2)      AS incc_var_mes_dez25,
    ROUND(i25.var_12_meses::numeric, 2) AS incc_var_12m_dez25,
    ROUND(i24.var_12_meses::numeric, 2) AS incc_var_12m_dez24
FROM sinapi_dez25 s25, sinapi_dez24 s24, incc_dez25 i25, incc_dez24 i24