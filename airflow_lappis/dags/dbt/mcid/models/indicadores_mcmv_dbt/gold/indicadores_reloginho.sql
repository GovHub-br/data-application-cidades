{{ config(materialized="table") }}

-- Gold do reloginho MCMV (grupo A): série mensal SNH deduplicada por APF.
-- Uma linha por (agente_financeiro, dt_referencia) com os acumulados
-- uh_contratadas / uh_entregues / uh_vigentes, n_apf (APFs distintos) e a
-- contagem corrida de meses observados por agente (n_meses_observados).
--
-- Fonte: parquets canônicos em staging/dados_historicos/ (conversão do dump
-- tratado, ver change reloginho-dados-historicos), lidos via
-- read_minio_staging_parquet_series (DuckDB + MinIO S3).
-- Target obrigatório: staging_duckdb (gating em dbt_project.yml).
--
-- dt_referencia vem do NOME DO ARQUIVO (primeiro dia do mês), mais confiável
-- que data_de_movimento (mantida só como auxiliar). Nomes canônicos usam
-- YYYYMM (ex. historico_recente_202603_...); alguns objetos BB preservam o
-- formato YYYY_MM (ex. historico_recente_2024_10_...) -> o separador é
-- normalizado antes do regexp_extract(\d{6}) / strptime(%Y%m).
--
-- CRÍTICO (#130 Fase 3): as tabelas historico_recente_* trazem cada APF
-- exatamente 2x no mesmo snapshot. Dedup com row_number() por
-- (agente_financeiro, apf, dt_referencia) mantendo rn = 1; sem isso os
-- totais dobram. Colunas numéricas são tratadas como texto na origem e
-- convertidas com try_cast(nullif(trim(...), '') as bigint).

with

fonte as (
    select
        filename,
        strptime(
            regexp_extract(
                regexp_replace(filename, '(20\d{2})_(\d{2})', '\1\2'),
                '(\d{6})',
                1
            ),
            '%Y%m'
        )::date as dt_referencia,
        upper(nullif(trim(agente_financeiro::text), '')) as agente_financeiro,
        nullif(trim(apf::text), '') as apf,
        try_cast(nullif(trim(data_de_movimento::text), '') as date) as data_de_movimento,
        try_cast(nullif(trim(uh_contratadas::text), '') as bigint) as uh_contratadas,
        try_cast(nullif(trim(uh_entregues::text), '') as bigint) as uh_entregues,
        try_cast(nullif(trim(uh_vigentes::text), '') as bigint) as uh_vigentes
    from {{ read_minio_staging_parquet_series('dados_historicos/historico_recente_*.parquet') }}
    where nullif(trim(apf::text), '') is not null
      and upper(nullif(trim(agente_financeiro::text), '')) is not null
),

dedup as (
    select
        *,
        row_number() over (
            partition by agente_financeiro, apf, dt_referencia
            order by data_de_movimento nulls last, filename
        ) as rn
    from fonte
    where dt_referencia is not null
),

mensal as (
    select
        agente_financeiro,
        dt_referencia,
        sum(uh_contratadas) as uh_contratadas,
        sum(uh_entregues) as uh_entregues,
        sum(uh_vigentes) as uh_vigentes,
        count(distinct apf) as n_apf
    from dedup
    where rn = 1
    group by agente_financeiro, dt_referencia
)

select
    dt_referencia,
    agente_financeiro,
    uh_contratadas,
    uh_entregues,
    uh_vigentes,
    n_apf,
    count(*) over (
        partition by agente_financeiro
        order by dt_referencia
    ) as n_meses_observados
from mensal
order by agente_financeiro, dt_referencia
