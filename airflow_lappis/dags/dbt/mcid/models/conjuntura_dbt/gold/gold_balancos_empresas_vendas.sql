{{ config(materialized='table') }}

WITH t_1t26 AS (

    SELECT
        nome_empresa,
        vendas
    FROM {{ ref('silver_balancos_empresas') }}
    WHERE ano_balanco = 2026
      AND trimestre_balanco = 1

),

t_4t25 AS (

    SELECT
        nome_empresa,
        vendas
    FROM {{ ref('silver_balancos_empresas') }}
    WHERE ano_balanco = 2025
      AND trimestre_balanco = 4

),

t_1t25 AS (

    SELECT
        nome_empresa,
        vendas
    FROM {{ ref('silver_balancos_empresas') }}
    WHERE ano_balanco = 2025
      AND trimestre_balanco = 1

),

-- trailing 12m até 1T26
acum_26 AS (

    SELECT
        nome_empresa,
        SUM(vendas) AS vendas_12m_26

    FROM {{ ref('silver_balancos_empresas') }}

    WHERE
        (ano_balanco = 2025 AND trimestre_balanco IN (2,3,4))
        OR
        (ano_balanco = 2026 AND trimestre_balanco = 1)

    GROUP BY nome_empresa

),

-- trailing 12m até 1T25
acum_25 AS (

    SELECT
        nome_empresa,
        SUM(vendas) AS vendas_12m_25

    FROM {{ ref('silver_balancos_empresas') }}

    WHERE
        (ano_balanco = 2024 AND trimestre_balanco IN (2,3,4))
        OR
        (ano_balanco = 2025 AND trimestre_balanco = 1)

    GROUP BY nome_empresa

),

-- trailing 12m até 1T24
acum_24 AS (

    SELECT
        nome_empresa,
        SUM(vendas) AS vendas_12m_24

    FROM {{ ref('silver_balancos_empresas') }}

    WHERE
        (ano_balanco = 2023 AND trimestre_balanco IN (2,3,4))
        OR
        (ano_balanco = 2024 AND trimestre_balanco = 1)

    GROUP BY nome_empresa

)

SELECT
    atual.nome_empresa,

    ROUND(
        (
            (atual.vendas::numeric / NULLIF(q4.vendas, 0)) - 1
        ) * 100,
        0
    ) AS variacao_4t25,

    ROUND(
        (
            (atual.vendas::numeric / NULLIF(t25.vendas, 0)) - 1
        ) * 100,
        0
    ) AS variacao_1t25,

    ROUND(
        (
            (a26.vendas_12m_26::numeric / NULLIF(a25.vendas_12m_25, 0)) - 1
        ) * 100,
        0
    ) AS variacao_12m_26_25,

    ROUND(
        (
            (a25.vendas_12m_25::numeric / NULLIF(a24.vendas_12m_24, 0)) - 1
        ) * 100,
        0
    ) AS variacao_12m_25_24

FROM t_1t26 atual

LEFT JOIN t_4t25 q4
    ON atual.nome_empresa = q4.nome_empresa

LEFT JOIN t_1t25 t25
    ON atual.nome_empresa = t25.nome_empresa

LEFT JOIN acum_26 a26
    ON atual.nome_empresa = a26.nome_empresa

LEFT JOIN acum_25 a25
    ON atual.nome_empresa = a25.nome_empresa

LEFT JOIN acum_24 a24
    ON atual.nome_empresa = a24.nome_empresa

ORDER BY
    CASE atual.nome_empresa
        WHEN 'MRV' THEN 1
        WHEN 'Cury' THEN 2
        WHEN 'Tenda' THEN 3
        WHEN 'Direcional' THEN 4
        WHEN 'Pacaembu' THEN 5
        WHEN 'Plano & Plano' THEN 6
    END