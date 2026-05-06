{{ config(materialized='table') }}

WITH construcao AS (
    SELECT
        periodo,
        data_referencia,
        -- Ano e mês do período móvel
        LEFT(periodo, 4)::int           AS ano,
        RIGHT(periodo, 2)::int          AS mes,
        -- Trimestre móvel baseado no mês central
        CASE
            WHEN RIGHT(periodo, 2)::int IN (1,2,3)   THEN 1
            WHEN RIGHT(periodo, 2)::int IN (4,5,6)   THEN 2
            WHEN RIGHT(periodo, 2)::int IN (7,8,9)   THEN 3
            WHEN RIGHT(periodo, 2)::int IN (10,11,12) THEN 4
        END                             AS trimestre,
        MAX(CASE WHEN categoria_id = '47949' THEN valor END) AS ocupados_construcao,
        MAX(CASE WHEN categoria_id = '47946' THEN valor END) AS ocupados_total
    FROM {{ ref('bronze_ibge_pnadc_ocupados_construcao') }}
    GROUP BY periodo, data_referencia
)

SELECT
    periodo,
    data_referencia,
    ano,
    mes,
    trimestre,
    ocupados_construcao,
    ocupados_total,
    -- Variação vs período anterior
    ROUND(
        ((ocupados_construcao / NULLIF(LAG(ocupados_construcao) OVER (ORDER BY periodo), 0)) - 1) * 100, 1
    ) AS var_mes,
    -- Variação vs mesmo período ano anterior (12 meses atrás)
    ROUND(
        ((ocupados_construcao / NULLIF(LAG(ocupados_construcao, 12) OVER (ORDER BY periodo), 0)) - 1) * 100, 1
    ) AS var_ano
FROM construcao