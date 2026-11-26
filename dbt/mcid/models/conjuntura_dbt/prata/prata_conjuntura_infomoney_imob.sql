{{ config(materialized='table') }}

-- Prata do conjuntura: Índice IMOB (Infomoney/Alpha Vantage).
-- A API devolve só os ~100 últimos pregões por carga, então a bronze guarda todas as
-- cargas (incluída a exportação única do histórico do Postgres) e aqui fica uma linha por
-- pregão, a da carga mais recente. A staging é texto (ADR-0002): a tipagem é aqui.
with
    cargas as (
        select
            *, row_number() over (partition by symbol, data_pregao order by _load_id desc) as ordem
        from {{ ref('bronze_infomoney_imob') }}
    )

select
    symbol,
    data_pregao::date as data_pregao,
    {{ parse_financial_value('open::text') }} as open,
    {{ parse_financial_value('high::text') }} as high,
    {{ parse_financial_value('low::text') }} as low,
    {{ parse_financial_value('close::text') }} as close,
    {{ parse_financial_value('volume::text') }} as volume,
    left(_ingested_at, 19)::timestamp as dt_ingest
from cargas
where ordem = 1
