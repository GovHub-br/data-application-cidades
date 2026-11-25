{{ config(materialized='table') }}

-- Prata do conjuntura: ICST (FGV-IBRE).
-- pg_duckdb lê o parquet tipado da staging (MinIO) direto do Postgres.
-- A fonte publica `mes` como texto "MM/YYYY" e os índices como texto com
-- vírgula decimal (pt-BR) — tipados aqui pra não repetir esse cast em todo
-- ouro que usar essa série, e pra não sofrer o bug de ordenação
-- lexicográfica de "MM/YYYY" (agrupa por mês antes de ano).
-- A bronze guarda todas as cargas; cada carga traz a série inteira, então fica a última.
select
    strptime(mes::text, '%m/%Y')::date as data_referencia,
    mes::text as periodo,
    replace(icst_com_ajuste_sazonal::text, ',', '.')::numeric as icst_com_ajuste_sazonal,
    replace(icst_sem_ajuste_sazonal::text, ',', '.')::numeric as icst_sem_ajuste_sazonal,
    _ingested_at::text as dt_ingest
from {{ ref('bronze_fgv_icst') }}
where {{ ultima_carga(ref('bronze_fgv_icst')) }}
