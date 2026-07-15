-- models/conjuntura_silver/silver_ibge_pmc_construcao.sql
{{ config(materialized='table') }}

with base as (
    select
        variavel_id,
        variavel_nome,
        data_referencia,
        valor,
        dt_ingest
    from {{ ref('bronze_ibge_pmc_construcao') }}
)

select
    variavel_id,
    variavel_nome,
    data_referencia,
    valor,
    {{ add_metadata_timestamps('silver') }}
from base