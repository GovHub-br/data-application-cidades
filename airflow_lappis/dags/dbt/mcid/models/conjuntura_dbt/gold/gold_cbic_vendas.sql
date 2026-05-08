{{ config(materialized='table') }}

WITH base AS (

    SELECT
        ano,
        trimestre,
        cbic_vendas_total   AS total,
        cbic_vendas_mcmv    AS mcmv,
        cbic_vendas_demais  AS demais

    FROM {{ ref('silver_cbic_lancamentos_vendas') }}

),

periodos AS (

    SELECT
        1 AS ordem,
        '4º TRI 2025' AS periodo,
        total,
        mcmv,
        demais
    FROM base
    WHERE ano = 2025
      AND trimestre = 4

    UNION ALL

    SELECT
        2,
        '3º TRI 2025',
        total,
        mcmv,
        demais
    FROM base
    WHERE ano = 2025
      AND trimestre = 3

    UNION ALL

    SELECT
        3,
        '4º TRI 2024',
        total,
        mcmv,
        demais
    FROM base
    WHERE ano = 2024
      AND trimestre = 4

    UNION ALL

    SELECT
        4,
        '12 MESES - DEZ/2025',
        SUM(total),
        SUM(mcmv),
        SUM(demais)
    FROM base
    WHERE ano = 2025

    UNION ALL

    SELECT
        5,
        '12 MESES - DEZ/2024',
        SUM(total),
        SUM(mcmv),
        SUM(demais)
    FROM base
    WHERE ano = 2024
)

SELECT
    periodo,
    total,
    mcmv,
    demais
FROM periodos
ORDER BY ordem