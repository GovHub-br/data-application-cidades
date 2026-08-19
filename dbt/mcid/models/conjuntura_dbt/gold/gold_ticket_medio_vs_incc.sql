{{ config(materialized='table') }}

WITH incc_tri AS (
    SELECT
        ano,
        trimestre,
        MAX(CASE WHEN EXTRACT(MONTH FROM data_referencia) IN (3,6,9,12) THEN var_tri END) AS incc_var_tri,
        MAX(CASE WHEN EXTRACT(MONTH FROM data_referencia) IN (3,6,9,12) THEN indice  END) AS indice,
        MAX(dt_ingest)                                                                     AS dt_ingest,
        MAX(dt_silver)                                                                     AS dt_silver
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
        t.dt_ingest,
        t.dt_silver,
        ROUND(((t.indice / b.indice_base) - 1) * 100, 1) AS incc_acum_4t20
    FROM incc_tri t, base_4t20 b
),

resultado AS (
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
        ROUND(e.ten_acum::numeric, 1)       AS ten_acum_4t20,
        GREATEST(e.dt_ingest, i.dt_ingest)  AS dt_ingest,
        GREATEST(e.dt_silver, i.dt_silver)  AS dt_silver
    FROM {{ ref('silver_ticket_medio_empresas') }} e
    LEFT JOIN incc_com_acum i
        ON e.ano = i.ano
        AND e.trimestre = i.trimestre
    WHERE (e.ano > 2023) OR (e.ano = 2023 AND e.trimestre = 4)
)

SELECT
    ano, trimestre,
    incc_var_tri, mrv_var_tri, dir_var_tri, ten_var_tri,
    incc_acum_4t20, mrv_acum_4t20, dir_acum_4t20, ten_acum_4t20,
    {{ add_metadata_timestamps('gold') }}
FROM resultado
ORDER BY ano, trimestre