{{ config(
    materialized='external',
    location='s3://poc-lake/bronze/bronze_clientes_txt.parquet',
    options={'compression': 'snappy'}
) }}

-- Idêntico ao bronze_clientes_csv, trocando apenas a source. Isso é o achado, não um
-- descuido: o SQL de tipagem não depende do formato de origem, só a LEITURA depende.
-- Com centenas de arquivos em raw/, isso vira argumento para codegen a partir de um
-- manifesto — ou para uma bronze genérica que não tipa nada (ver R3).

select
    try_cast(nullif(trim(codigo_cliente), '') as integer)  as codigo_cliente,
    nullif(trim(nome_do_cliente), '')                      as nome_do_cliente,
    poc_parse_date_br(data_de_cadastro)                    as data_de_cadastro,
    poc_parse_valor_br(valor_do_contrato)                  as valor_do_contrato,
    poc_sn_para_bool(ativo)                                as ativo,
    nullif(trim(cpf), '')                                  as cpf,
    nullif(trim(observacao), '')                           as observacao,
    filename                                               as _source_file,
    cast('{{ var("ingested_at") }}' as timestamp)          as _ingested_at

from {{ source('poc_raw', 'clientes_txt') }}
