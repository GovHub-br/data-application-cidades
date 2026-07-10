{{ config(materialized="table") }}

with fgv_icst_bronze as (
    select 
        cast(mes as text) as mes,
        to_date('01/' || mes, 'DD/MM/YYYY') as data_referencia,
        cast(replace(icst_com_ajuste_sazonal, ',', '.') as numeric) as icst_com_ajuste_sazonal,
        cast(replace(icst_sem_ajuste_sazonal, ',', '.') as numeric) as icst_sem_ajuste_sazonal,
        cast(dt_ingest as timestamp) as dt_ingest

    from {{ source("fgv", "icst") }}
)

select * 
from fgv_icst_bronze