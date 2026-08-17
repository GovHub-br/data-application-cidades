{{ config(
    materialized='external',
    location='s3://poc-lake/bronze/bronze_clientes_csv.parquet',
    options={'compression': 'snappy'}
) }}

-- Artefato C: a bronze TIPADA, escrita direto como parquet no MinIO.
-- Sem parquet TEXT intermediário e sem tabela VARCHAR no Postgres.
--
-- Estratégia: all_varchar=true na leitura (source) + TRY_CAST aqui. Preserva a propriedade
-- mais valiosa do pipeline atual — a ingestão nunca falha por inferência de tipo errada —
-- e concentra a tipagem num lugar só. A alternativa (columns={...} no read_csv) aborta o
-- arquivo inteiro num único valor ruim.

select
    try_cast(nullif(trim(codigo_cliente), '') as integer)  as codigo_cliente,
    nullif(trim(nome_do_cliente), '')                      as nome_do_cliente,
    poc_parse_date_br(data_de_cadastro)                    as data_de_cadastro,
    poc_parse_valor_br(valor_do_contrato)                  as valor_do_contrato,
    poc_sn_para_bool(ativo)                                as ativo,
    nullif(trim(cpf), '')                                  as cpf,
    nullif(trim(observacao), '')                           as observacao,

    -- Linhagem, paridade com LINEAGE_COLS do raw_para_staging.py.
    -- _source_hash não tem equivalente barato em SQL (read_blob carregaria o arquivo
    -- inteiro em memória) — fica como lacuna que exige passo Python. Ver R9.
    filename                                               as _source_file,
    cast('{{ var("ingested_at") }}' as timestamp)          as _ingested_at

from {{ source('poc_raw', 'clientes_csv') }}
