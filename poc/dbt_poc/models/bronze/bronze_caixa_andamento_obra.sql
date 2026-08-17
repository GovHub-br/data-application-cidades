{{ config(
    materialized='external',
    location='s3://poc-lake/bronze/caixa_andamento_obra',
    options={'format': 'parquet', 'compression': 'snappy', 'partition_by': 'anomes', 'overwrite_or_ignore': 1}
) }}

-- GERADO por scripts/gerar_models_bronze.py a partir de:
--   manifesto._manifesto_bronze  (quais arquivos, com que encoding/delimitador)
--   tipos/amostra_caixa_af_gehis_andamento_obra_m.yml  (a semântica de cada coluna — o artefato humano)
-- Não editar à mão: regenerar.
--
-- Família 'amostra_caixa_af_gehis_andamento_obra_m': 3 arquivo(s) em 2 grupo(s) de leitura.

with fonte as (

    -- 2 arquivo(s): encoding=cp1252, delim='|'
    select * from read_csv(
        [
            's3://poc-lake/raw/amostra__CAIXA_AF_GEHIS_ANDAMENTO_OBRA_M20240627.TXT',
            's3://poc-lake/raw/amostra__CAIXA_AF_GEHIS_ANDAMENTO_OBRA_M20250612.TXT'
        ],
        delim='|', header=true, all_varchar=true,
        encoding='cp1252', union_by_name=true, null_padding=true,
        filename=true, normalize_names=true
    )
    union all by name
    -- 1 arquivo(s): encoding=cp1252, delim=';'
    select * from read_csv(
        [
            's3://poc-lake/raw/amostra__CAIXA_AF_GEHIS_ANDAMENTO_OBRA_M20250424.txt'
        ],
        delim=';', header=true, all_varchar=true,
        encoding='cp1252', union_by_name=true, null_padding=true,
        filename=true, normalize_names=true
    )

)

select
    try_cast(nullif(trim(anomes), '') as integer)                  as anomes,
    poc_normalize_apf(nu_apf)                                      as nu_apf,
    poc_parse_date_br(dt_prevista_conclusao)                       as dt_prevista_conclusao,
    poc_parse_date_br(dt_prevista_inauguracao)                     as dt_prevista_inauguracao,
    nullif(trim(situacao_obra), '')                                as situacao_obra,
    filename                                                   as _source_file,
    cast('{{ var("ingested_at") }}' as timestamp)              as _ingested_at
from fonte
