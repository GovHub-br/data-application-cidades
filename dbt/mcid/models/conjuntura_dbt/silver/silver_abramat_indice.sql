{{ config(materialized="table") }}

select
    data_referencia,
    indice,
    var_mes,
    var_12_meses,
    {{ add_metadata_timestamps("silver") }}
from {{ source("conjuntura_bronze", "bronze_abramat_indice") }}
