{{ config(materialized='table') }}

with termino_obra as (
    select * from {{ source('fgts', 'fgts_canal_tab_ao_3_acompanhamento_termino_obra') }}
)

select
    operacao::varchar,
    numero_do_contrato::varchar,
    
    to_timestamp(nullif(data_termino_obra, '1900-01-01 00:00:00'), 'MM/DD/YY HH24:MI:SS') as data_termino_obra,
    to_timestamp(nullif(data_entrega_para_pf, '1900-01-01 00:00:00'), 'MM/DD/YY HH24:MI:SS') as data_entrega_pf
    
from termino_obra
