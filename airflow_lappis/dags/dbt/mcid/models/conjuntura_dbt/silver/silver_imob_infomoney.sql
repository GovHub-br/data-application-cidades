{{ config(materialized='table') }}

WITH mensal AS (
    SELECT
        DATE_TRUNC('month', data_pregao)::date  AS data_referencia,
        LAST_VALUE(close) OVER (
            PARTITION BY DATE_TRUNC('month', data_pregao)
            ORDER BY data_pregao
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        )                                        AS close_fim_mes
    FROM {{ source('conjuntura_bronze', 'bronze_imob_infomoney') }}
    WHERE symbol = 'IMOB.SA'
),

distinct_mensal AS (
    SELECT DISTINCT data_referencia, close_fim_mes
    FROM mensal
),

com_variacoes AS (
    SELECT
        data_referencia,
        close_fim_mes,
        ROUND(
            ((close_fim_mes / NULLIF(LAG(close_fim_mes, 1) OVER (ORDER BY data_referencia), 0)) - 1) * 100,
            1
        ) AS var_mes,
        ROUND(
            ((close_fim_mes / NULLIF(LAG(close_fim_mes, 12) OVER (ORDER BY data_referencia), 0)) - 1) * 100,
            1
        ) AS var_12_meses,
        ROUND(
            ((close_fim_mes / NULLIF(FIRST_VALUE(close_fim_mes) OVER (ORDER BY data_referencia ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), 0)) - 1) * 100,
            1
        ) AS var_acum_serie
    FROM distinct_mensal
)

SELECT * FROM com_variacoes