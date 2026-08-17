{{ config(
    materialized='table',
    database='pg',
    schema='silver',
    alias='clientes'
) }}

-- Artefato E: silver materializada no POSTGRES, mas o SQL roda no DuckDB — o Postgres é
-- só destino. Os ref() apontam para models `external`, que o dbt-duckdb expõe como view
-- sobre o parquet do MinIO: a silver lê o parquet tipado direto, sem tabela intermediária.
-- É essa propriedade que elimina a "segunda bronze".
--
-- Repare no `qualify`: é dialeto DuckDB. Adotar esta arquitetura significa que todo o SQL
-- de silver/gold passa a ser DuckDB, não Postgres (R1).

with unificado as (

    select 'csv' as origem, * from {{ ref('bronze_clientes_csv') }}
    union all
    select 'txt' as origem, * from {{ ref('bronze_clientes_txt') }}
    union all
    select 'xlsx' as origem, * from {{ ref('bronze_clientes_xlsx') }}

)

select
    codigo_cliente,
    nome_do_cliente,
    data_de_cadastro,
    valor_do_contrato,
    ativo,
    cpf,
    observacao,
    origem,
    _source_file,
    _ingested_at
from unificado
qualify row_number() over (
    partition by codigo_cliente, origem order by _ingested_at desc
) = 1
