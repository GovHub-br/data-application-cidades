-- models/conjuntura_silver/silver_ibge_pim_pf_brasil.sql
{{ config(materialized='table') }}

with base as (
    select
        variavel_id,
        variavel_nome,
        data_referencia,
        valor,
        dt_ingest
    from {{ ref('bronze_ibge_pim_pf_brasil') }}
)

select
    variavel_id,
    variavel_nome,
    data_referencia,
    valor,
    {{ add_metadata_timestamps('silver') }}
from base