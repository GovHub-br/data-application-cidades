{{ config(materialized='table') }}

with entidades as (
    select * from {{ source('fgts', 'fgts_canal_tdom_ao_1_entidades') }}
)

select
    codigo::varchar,
    cgc::varchar as cnpj,
    entidade::varchar as nome_entidade
from entidades
