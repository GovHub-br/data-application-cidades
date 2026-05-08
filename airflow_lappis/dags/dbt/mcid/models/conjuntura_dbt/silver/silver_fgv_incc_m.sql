{{ config(materialized='table') }}

SELECT
    mes,
    data_referencia,
    indice,
    var_ano,
    var_mes,
    var_12_meses,
    -- Variação trimestral (índice fim tri / índice 3 meses antes)
    ROUND(
        ((indice / NULLIF(LAG(indice, 3) OVER (ORDER BY data_referencia), 0)) - 1) * 100,
        1
    ) AS var_tri,
    -- Trimestre
    CASE
        WHEN EXTRACT(MONTH FROM data_referencia) IN (1,2,3)   THEN 1
        WHEN EXTRACT(MONTH FROM data_referencia) IN (4,5,6)   THEN 2
        WHEN EXTRACT(MONTH FROM data_referencia) IN (7,8,9)   THEN 3
        WHEN EXTRACT(MONTH FROM data_referencia) IN (10,11,12) THEN 4
    END AS trimestre,
    EXTRACT(YEAR FROM data_referencia)::int AS ano,
    dt_ingest
FROM {{ source('conjuntura_bronze', 'bronze_fgv_incc_m') }}