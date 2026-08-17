{{ config(
    materialized='external',
    location='s3://poc-lake/bronze/clientes_cp_patologico',
    options={'format': 'parquet', 'compression': 'snappy'}
) }}

-- GERADO por scripts/gerar_models_bronze.py a partir de:
--   manifesto._manifesto_bronze  (quais arquivos, com que encoding/delimitador)
--   tipos/clientes_cp_patologico.yml  (a semântica de cada coluna — o artefato humano)
-- Não editar à mão: regenerar.
--
-- Família 'clientes_cp_patologico': 1 arquivo(s) em 1 grupo(s) de leitura.

with fonte as (

    -- 1 arquivo(s): encoding=utf-8, delim=';'
    select * from read_csv(
        [
            's3://poc-lake/raw_utf8/clientes_cp1252_patologico.csv'
        ],
        delim=';', header=true, all_varchar=true,
        encoding='utf-8', union_by_name=true, null_padding=true,
        filename=true, normalize_names=true
    )

)

select
    nullif(trim(codigo_cliente), '')                               as codigo_cliente,
    nullif(trim(nome_do_cliente), '')                              as nome_do_cliente,
    poc_parse_date_br(data_de_cadastro)                            as data_de_cadastro,
    poc_parse_valor_br(valor_do_contrato)                          as valor_do_contrato,
    poc_sn_para_bool(ativo)                                        as ativo,
    nullif(trim(cpf), '')                                          as cpf,
    nullif(trim(observacao), '')                                   as observacao,
    filename                                                   as _source_file,
    cast('{{ var("ingested_at") }}' as timestamp)              as _ingested_at
from fonte
