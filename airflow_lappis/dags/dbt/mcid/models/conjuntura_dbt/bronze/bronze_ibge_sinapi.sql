{{ config(materialized="table") }}

with ibge_sinapi_bronze as (
    select 
        cast(variavel_id as integer) as variavel_id,
        cast(variavel_nome as text) as variavel_nome,

        cast(localidade_id as integer) as localidade_id,
        upper(trim(cast(localidade_nome as text))) as localidade,

        cast(classificacao_id as integer) as classificacao_id,
        upper(trim(cast(classificacao_nome as text))) as classificacao,

        cast(categoria_id as integer) as categoria_id,
        upper(trim(cast(categoria_nome as text))) as categoria,

        cast (unidade as text) as unidade,
        cast(periodo as text) as periodo,
        to_date(periodo || '01', 'YYYYMMDD') as data_referencia,
        cast(
                replace(
                    replace(valor, '.', ''),
                    ',', '.' 
            ) as numeric
        ) as valor,
        
        cast(dt_ingest as timestamp) as dt_ingest
    
    from {{ source("ibge", "sinapi") }}
)

select * 
from ibge_sinapi_bronze