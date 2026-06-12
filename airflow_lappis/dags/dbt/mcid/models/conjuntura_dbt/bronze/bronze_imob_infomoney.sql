{{ config(materialized="table") }}

select 
    cast(symbol as varchar) as symbol,
    cast(data_pregao as date) as data_pregao,
    cast(nullif(replace(replace(trim("open"), '.', ''), ',', '.'), '') as numeric) as open,
    cast(nullif(replace(replace(trim("high"), '.', ''), ',', '.'), '') as numeric) as high,
    cast(nullif(replace(replace(trim("low"), '.', ''), ',', '.'), '') as numeric) as low,
    cast(nullif(replace(replace(trim("close"), '.', ''), ',', '.'), '') as numeric) as close,
    cast(nullif(trim(volume), '') as bigint) as volume,
    cast(dt_ingest as timestamp) as dt_ingest
from {{ source('infomoney', 'acoes_imob') }}