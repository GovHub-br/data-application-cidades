{{ config(materialized="table") }}

select
    cast(variavel_id as int) as variavel_id,
    cast(variavel_nome as text) as variavel_nome,
    cast(localidade_id as int) as localidade_id,
    cast(localidade_nome as text) as localidade_nome,
    cast(classificacao_id as text) as classificacao_id,
    cast(classificacao_nome as text) as classificacao_nome,
    cast(categoria_id as text) as categoria_id,
    cast(categoria_nome as text) as categoria_nome,
    cast(unidade as text) as unidade,
    cast(periodo as text) as periodo,
    to_date(periodo, 'YYYYMM') as data_referencia,
    cast(valor as numeric) as valor,
    cast(dt_ingest as timestamp) as dt_ingest
from {{ source("ibge", "pnadc_ocupados_construcao") }}
