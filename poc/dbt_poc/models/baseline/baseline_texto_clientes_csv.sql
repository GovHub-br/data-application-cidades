{{ config(
    materialized='external',
    location='s3://poc-lake/_baseline_text/clientes_csv.parquet',
    options={'compression': 'snappy'}
) }}

-- Artefato B: o MESMO conteúdo escrito pelo MESMO engine (DuckDB) e MESMO codec (snappy),
-- porém todo VARCHAR. É o controle do experimento: comparando B com C isola-se o efeito da
-- TIPAGEM; comparando A com B isola-se o efeito do ENGINE (pandas/pyarrow vs DuckDB).
-- Sem esse artefato, qualquer diferença entre A e C seria atribuída à tipagem por engano.

select
    codigo_cliente,
    nome_do_cliente,
    data_de_cadastro,
    valor_do_contrato,
    ativo,
    cpf,
    observacao,
    filename                                       as _source_file,
    cast('{{ var("ingested_at") }}' as timestamp)  as _ingested_at

from {{ source('poc_raw', 'clientes_csv') }}
