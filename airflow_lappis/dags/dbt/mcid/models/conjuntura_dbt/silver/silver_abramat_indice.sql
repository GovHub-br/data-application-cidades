{{ config(materialized='table') }}

SELECT
    data_referencia,
    indice,
    var_mes,
    var_12_meses,
    dt_ingest
FROM {{ source('conjuntura_bronze', 'bronze_abramat_indice') }}