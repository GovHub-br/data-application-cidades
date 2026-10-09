{{ config(materialized='table') }}

-- Prata do conjuntura: Índice IMOB (Infomoney/Alpha Vantage).
-- pg_duckdb lê o parquet tipado da staging (MinIO) direto do Postgres.
-- Parquet já sai tipado da ingestão (Etapa 02), então a prata é passthrough.
-- Full-refresh: cada run reconstrói a tabela a partir do parquet atual.
--
-- Ingestão nova (plugins/ingestion, 10/2026): um arquivo por símbolo na staging
-- (`<símbolo>.parquet`), com as colunas que o Alpha Vantage dá (`1. open`…). O
-- bronze em merge junta as ingestões, inclusive a partição inicial com o
-- histórico que a ingestão antiga acumulava no Postgres. O símbolo sai do nome do
-- arquivo.

select
    regexp_replace(filename, '^.*/([^/]+)\.parquet$', '\1')   as symbol,
    data_pregao::date                                          as data_pregao,
    {{ parse_financial_value('"1. open"::text') }}             as open,
    {{ parse_financial_value('"2. high"::text') }}             as high,
    {{ parse_financial_value('"3. low"::text') }}              as low,
    {{ parse_financial_value('"4. close"::text') }}            as close,
    {{ parse_financial_value('"5. volume"::text') }}           as volume,
    {{ lake_dt_ingest() }}                                     as dt_ingest
from {{ ref('bronze_infomoney_imob') }}
