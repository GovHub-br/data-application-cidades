{{ config(materialized="table") }}

with bacen_financiamentos_imobiliarios_bronze as (
    select 
        cast(tipo as text) as tipo,
        cast(data as text) as data,
        to_date(data || '01', 'DD/MM/YYYY') as data_referencia,
        cast(valor as numeric) as valor,
        cast(dt_ingest as timestamp) as dt_ingest
    
    from {{ source("bacen", "financiamentos_imobiliarios") }}
)

select *
from bacen_financiamentos_imobiliarios_bronze