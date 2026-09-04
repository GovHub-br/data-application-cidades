{{ config(materialized='table') }}

-- Silver do conjuntura contínuo: Índice IMOB (Infomoney/Alpha Vantage).
-- pg_duckdb lê o parquet tipado da staging (MinIO) direto do Postgres.
-- Parquet já sai tipado da ingestão (Etapa 02), então a silver é passthrough.
-- Full-refresh: cada run reconstrói a tabela a partir do parquet atual.

select
    symbol,
    data_pregao::date as data_pregao,
    {{ parse_financial_value('open::text') }} as open,
    {{ parse_financial_value('high::text') }} as high,
    {{ parse_financial_value('low::text') }} as low,
    {{ parse_financial_value('close::text') }} as close,
    {{ parse_financial_value('volume::text') }} as volume,
    dt_ingest::timestamp as dt_ingest
from {{ ref('bnz_infomoney_imob') }}
