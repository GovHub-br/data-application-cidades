{{ config(materialized='table') }}

-- Silver do conjuntura contínuo: ICST (FGV-IBRE).
-- pg_duckdb lê o parquet tipado da staging (MinIO) direto do Postgres.
-- A fonte publica `mes` como texto "MM/YYYY" e os índices como texto com
-- vírgula decimal (pt-BR) — tipados aqui pra não repetir esse cast em todo
-- gold que usar essa série, e pra não sofrer o bug de ordenação
-- lexicográfica de "MM/YYYY" (agrupa por mês antes de ano).

select
    to_date(mes, 'MM/YYYY')                                        as data_referencia,
    mes                                                             as periodo,
    replace(icst_com_ajuste_sazonal, ',', '.')::numeric             as icst_com_ajuste_sazonal,
    replace(icst_sem_ajuste_sazonal, ',', '.')::numeric             as icst_sem_ajuste_sazonal,
    dt_ingest
from read_parquet('s3://data-lake-mcid/staging/fgv/icst.parquet')
