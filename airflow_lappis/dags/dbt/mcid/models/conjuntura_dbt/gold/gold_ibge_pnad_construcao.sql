{{ config(materialized='table') }}

WITH ocupados AS (
    SELECT
        ano, mes, trimestre, periodo,
        ocupados_construcao, ocupados_total,
        var_mes, var_ano
    FROM {{ ref('silver_ibge_pnadc_ocupados_construcao') }}
    WHERE RIGHT(periodo, 2)::int IN (3, 6, 9, 12)
),

rendimento AS (
    SELECT
        periodo,
        rendimento_construcao, rendimento_total,
        var_mes AS rend_var_mes, var_ano AS rend_var_ano
    FROM {{ ref('silver_ibge_pnadc_rendimento_construcao') }}
    WHERE RIGHT(periodo, 2)::int IN (3, 6, 9, 12)
)

SELECT
    o.ano, o.trimestre, o.periodo,
    o.ocupados_construcao               AS pnad_const_milhares,
    o.ocupados_total                    AS pnad_total_milhares,
    o.var_mes                           AS pnad_const_var_mes,
    o.var_ano                           AS pnad_const_var_ano,
    r.rendimento_construcao             AS rend_const_rs,
    r.rendimento_total                  AS rend_total_rs,
    r.rend_var_mes                      AS rend_const_var_mes,
    r.rend_var_ano                      AS rend_const_var_ano
FROM ocupados o
LEFT JOIN rendimento r ON o.periodo = r.periodo
ORDER BY o.ano, o.trimestre