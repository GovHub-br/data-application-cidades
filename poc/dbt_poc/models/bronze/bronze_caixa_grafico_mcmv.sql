{{ config(
    materialized='external',
    location='s3://poc-lake/bronze/caixa_grafico_mcmv',
    options={'format': 'parquet', 'compression': 'snappy'}
) }}

-- GERADO por scripts/gerar_models_bronze.py a partir de:
--   manifesto._manifesto_bronze  (quais arquivos, com que encoding/delimitador)
--   tipos/amostra_dados_historicos_caixa_grafico_mcmv.yml  (a semântica de cada coluna — o artefato humano)
-- Não editar à mão: regenerar.
--
-- Família 'amostra_dados_historicos_caixa_grafico_mcmv': 1 arquivo(s) em 1 grupo(s) de leitura.

with fonte as (

    -- 1 arquivo(s): encoding=utf-8, delim=';'
    select * from read_csv(
        [
            's3://poc-lake/raw/amostra__dados_historicos__caixa_001_2016_grafico_mcmv_31082016.csv'
        ],
        delim=';', header=true, all_varchar=true,
        encoding='utf-8', union_by_name=true, null_padding=true,
        filename=true, normalize_names=true
    )

)

select
    nullif(trim(unnamed_0), '')                                    as situacao,
    poc_parse_valor_br(unnamed_1)                                  as quantidade,
    nullif(trim(unnamed_8), '')                                    as rotulo_base,
    poc_parse_date_br(unnamed_9)                                   as data_base,
    filename                                                   as _source_file,
    cast('{{ var("ingested_at") }}' as timestamp)              as _ingested_at
from fonte
