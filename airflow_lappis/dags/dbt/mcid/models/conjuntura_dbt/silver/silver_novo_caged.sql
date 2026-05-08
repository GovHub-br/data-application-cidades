{{ config(materialized='table') }}

WITH base AS (
    SELECT
        ano,
        mes,
        admitidos,
        desligados,
        saldo,
        estoque,
        variacao,
        -- Trimestre
        CASE
            WHEN mes IN (1,2,3)   THEN 1
            WHEN mes IN (4,5,6)   THEN 2
            WHEN mes IN (7,8,9)   THEN 3
            WHEN mes IN (10,11,12) THEN 4
        END AS trimestre,
        -- Variação saldo vs mês anterior
        ROUND(
            ((saldo::numeric / NULLIF(LAG(saldo) OVER (ORDER BY ano, mes), 0)) - 1) * 100, 1
        ) AS saldo_var_mes,
        -- Variação estoque vs mesmo mês ano anterior
        ROUND(
            ((estoque::numeric / NULLIF(LAG(estoque, 12) OVER (ORDER BY ano, mes), 0)) - 1) * 100, 1
        ) AS estoque_var_ano
    FROM {{ ref('bronze_novo_caged') }}
)

SELECT * FROM base