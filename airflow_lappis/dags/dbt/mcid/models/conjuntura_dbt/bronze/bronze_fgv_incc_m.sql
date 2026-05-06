{{ config(materialized="table") }}

with fgv_incc_m_bronze as (
    select 
        cast(mes as timestamp) as mes,
        cast(mes as date) as data_referencia,

        cast(nullif(replace(indice, ',', '.'), '...') as numeric) as indice,

        cast(nullif(replace(var_ano, ',', '.'), '...') as numeric) as var_ano,

        cast(nullif(replace(var_mes, ',', '.'), '...') as numeric) as var_mes,

        cast(nullif(replace(var_12_meses, ',', '.'), '...') as numeric) as var_12_meses,

        cast(dt_ingest as timestamp) as dt_ingest

    from {{ source("fgv", "incc_m") }}
)

select *
from fgv_incc_m_bronze