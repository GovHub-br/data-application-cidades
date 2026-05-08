{{ config(materialized='table') }}

WITH t_4t25 AS (

    SELECT *
    FROM {{ ref('silver_financiamentos_habitacionais') }}
    WHERE ano = 2025
      AND trimestre = 4

),

t_3t25 AS (

    SELECT *
    FROM {{ ref('silver_financiamentos_habitacionais') }}
    WHERE ano = 2025
      AND trimestre = 3

),

t_4t24 AS (

    SELECT *
    FROM {{ ref('silver_financiamentos_habitacionais') }}
    WHERE ano = 2024
      AND trimestre = 4

),

acum_25 AS (

    SELECT
        SUM(fgts_uh) AS fgts_uh,
        SUM(sbpe_uh) AS sbpe_uh

    FROM {{ ref('silver_financiamentos_habitacionais') }}
    WHERE ano = 2025

),

acum_24 AS (

    SELECT
        SUM(fgts_uh) AS fgts_uh,
        SUM(sbpe_uh) AS sbpe_uh

    FROM {{ ref('silver_financiamentos_habitacionais') }}
    WHERE ano = 2024

)

SELECT
    '4º TRI 2025' AS periodo,
    fgts_uh,
    sbpe_uh
FROM t_4t25

UNION ALL

SELECT
    '3º TRI 2025',
    fgts_uh,
    sbpe_uh
FROM t_3t25

UNION ALL

SELECT
    '4º TRI 2024',
    fgts_uh,
    sbpe_uh
FROM t_4t24

UNION ALL

SELECT
    '12 MESES - DEZ/2025',
    fgts_uh,
    sbpe_uh
FROM acum_25

UNION ALL

SELECT
    '12 MESES - DEZ/2024',
    fgts_uh,
    sbpe_uh
FROM acum_24