{{ config(materialized='table') }}

WITH incc_tri AS (
    SELECT
        ano,
        trimestre,
        -- Pega a variação trimestral do último mês de cada trimestre
        MAX(CASE WHEN EXTRACT(MONTH FROM data_referencia) IN (3,6,9,12) THEN var_tri END) AS incc_var_tri,
        MAX(CASE WHEN EXTRACT(MONTH FROM data_referencia) IN (3,6,9,12) THEN indice  END) AS indice
    FROM {{ ref('silver_fgv_incc_m') }}
    GROUP BY ano, trimestre
),

base_4t20 AS (
    SELECT indice AS indice_base
    FROM {{ ref('silver_fgv_incc_m') }}
    WHERE data_referencia = '2020-12-01'
),

incc_com_acum AS (
    SELECT
        t.ano,
        t.trimestre,
        t.incc_var_tri,
        ROUND(((t.indice / b.indice_base) - 1) * 100, 1) AS incc_acum_4t20
    FROM incc_tri t, base_4t20 b
)

SELECT
    e.ano,
    e.trimestre,
    ROUND(i.incc_var_tri::numeric, 1)   AS incc_var_tri,
    ROUND(e.mrv_var_tri::numeric, 1)    AS mrv_var_tri,
    ROUND(e.dir_var_tri::numeric, 1)    AS dir_var_tri,
    ROUND(e.ten_var_tri::numeric, 1)    AS ten_var_tri,
    ROUND(i.incc_acum_4t20::numeric, 1) AS incc_acum_4t20,
    ROUND(e.mrv_acum::numeric, 1)       AS mrv_acum_4t20,
    ROUND(e.dir_acum::numeric, 1)       AS dir_acum_4t20,
    ROUND(e.ten_acum::numeric, 1)       AS ten_acum_4t20
FROM {{ ref('silver_ticket_medio_empresas') }} e
LEFT JOIN incc_com_acum i ON e.ano = i.ano AND e.trimestre = i.trimestre
WHERE (e.ano > 2023) OR (e.ano = 2023 AND e.trimestre = 4)
ORDER BY e.ano, e.trimestre