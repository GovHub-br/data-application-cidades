{{ config(materialized='table') }}

-- Prata do conjuntura: ICST (FGV-IBRE).
-- pg_duckdb lê o parquet tipado da staging (MinIO) direto do Postgres.
-- A fonte publica `mes` como texto "MM/YYYY" e os índices como texto com
-- vírgula decimal (pt-BR) — tipados aqui pra não repetir esse cast em todo
-- ouro que usar essa série, e pra não sofrer o bug de ordenação
-- lexicográfica de "MM/YYYY" (agrupa por mês antes de ano).
--
-- Ingestão nova (plugins/ingestion, 10/2026): a staging guarda o cabeçalho do
-- CSV como veio, com o nome longo de cada série e o código da FGV entre
-- parênteses; o renome é aqui. `dt_ingest` é a partição da ingestão.

select
    strptime("Data"::text, '%m/%Y')::date                          as data_referencia,
    "Data"::text                                                    as periodo,
    replace(
        "ICST Com ajuste Sazonal - Índice de Confiança da Construção (CNAE 2.0)(1416232)"::text,
        ',', '.'
    )::numeric                                                      as icst_com_ajuste_sazonal,
    replace(
        "ICST Sem ajuste Sazonal - Índice de Confiança da Construção (CNAE 2.0)(1416229)"::text,
        ',', '.'
    )::numeric                                                      as icst_sem_ajuste_sazonal,
    {{ lake_dt_ingest() }}                                          as dt_ingest
from {{ ref('bronze_fgv_icst') }}
