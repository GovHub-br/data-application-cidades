-- Tabela de ações IMOB extraída pelo alphavantage_imob_dag.py
select 
    cast(symbol as varchar) as symbol,
    cast(data_pregao as date) as data_pregao,
    cast("open" as numeric) as open,
    cast("high" as numeric) as high,
    cast("low" as numeric) as low,
    cast("close" as numeric) as close,
    cast(volume as bigint) as volume,
    cast(dt_ingest as timestamp) as dt_ingest
from {{ source('infomoney', 'acoes_imob') }}
