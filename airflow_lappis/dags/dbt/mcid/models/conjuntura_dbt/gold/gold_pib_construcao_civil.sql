{{ config(materialized='table') }}

WITH pivotado AS (

    SELECT
        variavel_id,

        MAX(CASE WHEN periodo = '202404' THEN valor_percentual END) AS t_4t24,
        MAX(CASE WHEN periodo = '202501' THEN valor_percentual END) AS t_1t25,
        MAX(CASE WHEN periodo = '202502' THEN valor_percentual END) AS t_2t25,
        MAX(CASE WHEN periodo = '202503' THEN valor_percentual END) AS t_3t25,
        MAX(CASE WHEN periodo = '202504' THEN valor_percentual END) AS t_4t25

    FROM {{ ref('silver_ibge_pib_construcao_civil') }}
    GROUP BY variavel_id
)

SELECT
    CASE variavel_id
        WHEN 6564 THEN 'Trim./Trim. Imediatamente Anterior'
        WHEN 6563 THEN 'Acumulada ao Longo do Ano'
        WHEN 6562 THEN 'Acum. Últimos 4 Trimestres'
    END AS indicador,

    ROUND(t_4t24, 1) AS tri_2024_4,
    ROUND(t_1t25, 1) AS tri_2025_1,
    ROUND(t_2t25, 1) AS tri_2025_2,
    ROUND(t_3t25, 1) AS tri_2025_3,
    ROUND(t_4t25, 1) AS tri_2025_4

FROM pivotado
ORDER BY variavel_id DESC