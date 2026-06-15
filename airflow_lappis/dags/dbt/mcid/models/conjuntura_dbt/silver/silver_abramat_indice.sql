{{ config(materialized='table') }}

SELECT
    data_referencia,
    indice,
    var_mes,
    var_12_meses,
    {{ add_metadata_timestamps('silver') }}
FROM {{ source('conjuntura_bronze', 'bronze_abramat_indice') }}