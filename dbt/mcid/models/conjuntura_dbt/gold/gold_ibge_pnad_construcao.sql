{{ config(materialized='table') }}

WITH ocupados AS (
    SELECT
        ano, mes, trimestre, periodo,
        ocupados_construcao, ocupados_total,
        var_mes, var_ano,
        dt_ingest, dt_silver
    FROM {{ ref('silver_ibge_pnadc_ocupados_construcao') }}
    WHERE RIGHT(periodo, 2)::int IN (3, 6, 9, 12)
),

rendimento AS (
    SELECT
        periodo,
        rendimento_construcao, rendimento_total,
        var_mes                 AS rend_var_mes,
        var_ano                 AS rend_var_ano,
        dt_ingest               AS dt_ingest_rend,
        dt_silver               AS dt_silver_rend
    FROM {{ ref('silver_ibge_pnadc_rendimento_construcao') }}
    WHERE RIGHT(periodo, 2)::int IN (3, 6, 9, 12)
),

resultado AS (
    SELECT
        o.ano, o.trimestre, o.periodo,
        o.ocupados_construcao               AS pnad_const_milhares,
        o.ocupados_total                    AS pnad_total_milhares,
        o.var_mes                           AS pnad_const_var_mes,
        o.var_ano                           AS pnad_const_var_ano,
        r.rendimento_construcao             AS rend_const_rs,
        r.rendimento_total                  AS rend_total_rs,
        r.rend_var_mes                      AS rend_const_var_mes,
        r.rend_var_ano                      AS rend_const_var_ano,
        GREATEST(o.dt_ingest, r.dt_ingest_rend)     AS dt_ingest,
        GREATEST(o.dt_silver, r.dt_silver_rend)     AS dt_silver
    FROM ocupados o
    LEFT JOIN rendimento r ON o.periodo = r.periodo
)

SELECT
    ano, trimestre, periodo,
    pnad_const_milhares,
    pnad_total_milhares,
    pnad_const_var_mes,
    pnad_const_var_ano,
    rend_const_rs,
    rend_total_rs,
    rend_const_var_mes,
    rend_const_var_ano,
    {{ add_metadata_timestamps('gold') }}
FROM resultado
ORDER BY ano, trimestre