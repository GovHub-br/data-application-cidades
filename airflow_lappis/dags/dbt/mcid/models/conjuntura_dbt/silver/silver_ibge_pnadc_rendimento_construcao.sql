{{ config(materialized='table') }}

WITH construcao AS (
    SELECT
        periodo,
        data_referencia,
        LEFT(periodo, 4)::int           AS ano,
        RIGHT(periodo, 2)::int          AS mes,
        CASE
            WHEN RIGHT(periodo, 2)::int IN (1,2,3)   THEN 1
            WHEN RIGHT(periodo, 2)::int IN (4,5,6)   THEN 2
            WHEN RIGHT(periodo, 2)::int IN (7,8,9)   THEN 3
            WHEN RIGHT(periodo, 2)::int IN (10,11,12) THEN 4
        END                             AS trimestre,
        MAX(CASE WHEN categoria_id = '47949' THEN valor END) AS rendimento_construcao,
        MAX(CASE WHEN categoria_id = '47946' THEN valor END) AS rendimento_total
    FROM {{ ref('bronze_ibge_pnadc_rendimento_construcao') }}
    GROUP BY periodo, data_referencia
)

SELECT
    periodo,
    data_referencia,
    ano,
    mes,
    trimestre,
    rendimento_construcao,
    rendimento_total,
    ROUND(
        ((rendimento_construcao / NULLIF(LAG(rendimento_construcao) OVER (ORDER BY periodo), 0)) - 1) * 100, 1
    ) AS var_mes,
    ROUND(
        ((rendimento_construcao / NULLIF(LAG(rendimento_construcao, 12) OVER (ORDER BY periodo), 0)) - 1) * 100, 1
    ) AS var_ano
FROM construcao