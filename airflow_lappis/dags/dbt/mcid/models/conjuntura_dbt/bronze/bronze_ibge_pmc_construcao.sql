-- models/conjuntura_bronze/bronze_ibge_pmc_construcao.sql
{{ config(materialized='table') }}

with ibge_pmc_construcao_bronze as (
    select
        cast(variavel_id as integer)                   as variavel_id,
        cast(variavel_nome as text)                    as variavel_nome,
        upper(trim(cast(localidade_nome as text)))     as localidade_nome,
        cast(classificacao_id as integer)               as classificacao_id,
        upper(trim(cast(classificacao_nome as text)))  as classificacao_nome,
        cast(categoria_id as integer)                   as categoria_id,
        upper(trim(cast(categoria_nome as text)))       as categoria_nome,
        cast(unidade as text)                           as unidade,
        cast(periodo as text)                           as periodo,
        to_date(periodo || '01', 'YYYYMMDD')            as data_referencia,
        cast(
            nullif(trim(valor), '-') as numeric
        )                                                as valor,
        cast(dt_ingest as timestamp)                    as dt_ingest
    from {{ source('ibge', 'ibge_pmc_construcao') }}
)
select * from ibge_pmc_construcao_bronze