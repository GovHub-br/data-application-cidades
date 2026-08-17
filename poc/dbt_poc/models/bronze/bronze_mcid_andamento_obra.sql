{{ config(
    materialized='external',
    location='s3://poc-lake/bronze/bronze_mcid_andamento_obra.parquet',
    options={'compression': 'snappy'}
) }}

-- Bronze tipada sobre uma AMOSTRA REAL do lake (cp1252, delimitador '|').
-- Exercita os UDFs portados sobre a semântica real do MCid:
--   nu_apf                  -> poc_normalize_apf  (porte de f_normalize_apf.sql)
--   dt_prevista_conclusao   -> poc_parse_date_br  (porte de f_parse_date_br.sql)

select
    try_cast(nullif(trim(anomes), '') as integer)      as anomes,
    poc_normalize_apf(nu_apf)                          as nu_apf,
    poc_parse_date_br(dt_prevista_conclusao)           as dt_prevista_conclusao,
    poc_parse_date_br(dt_prevista_inauguracao)         as dt_prevista_inauguracao,
    nullif(trim(situacao_obra), '')                    as situacao_obra,
    filename                                           as _source_file,
    cast('{{ var("ingested_at") }}' as timestamp)      as _ingested_at

from {{ source('poc_raw', 'mcid_andamento_obra') }}
