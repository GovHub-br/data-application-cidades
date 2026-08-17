{{ config(
    materialized='external',
    location='s3://poc-lake/bronze/bronze_clientes_xlsx.parquet',
    options={'compression': 'snappy'}
) }}

-- XLSX lido direto de s3:// — a extensão excel faz range requests via httpfs.
-- Duas diferenças em relação ao read_csv:
--   1. read_xlsx não tem normalize_names -> os nomes vêm crus, com acento e espaço,
--      e a normalização precisa ser feita à mão no SELECT
--   2. read_xlsx não tem a coluna virtual `filename` -> _source_file vira literal

select
    try_cast(nullif(trim("Código Cliente"), '') as integer)  as codigo_cliente,
    nullif(trim("Nome do Cliente"), '')                      as nome_do_cliente,
    poc_parse_date_br("Data de Cadastro")                    as data_de_cadastro,
    poc_parse_valor_br("Valor do Contrato")                  as valor_do_contrato,
    poc_sn_para_bool("Ativo")                                as ativo,
    nullif(trim("CPF"), '')                                  as cpf,
    nullif(trim("Observação"), '')                           as observacao,
    's3://poc-lake/raw/clientes.xlsx'                        as _source_file,
    cast('{{ var("ingested_at") }}' as timestamp)            as _ingested_at

from {{ source('poc_raw', 'clientes_xlsx') }}
