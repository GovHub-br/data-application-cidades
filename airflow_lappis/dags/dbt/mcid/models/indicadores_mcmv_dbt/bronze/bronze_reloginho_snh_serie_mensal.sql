{{ config(materialized="table") }}

-- BRONZE do reloginho MCMV (grupo A) — cópia fiel da série mensal SNH.
--
-- Empilha, sem regra de negócio, todos os snapshots mensais de "dados
-- prioritários" do SNH (`historico_recente_*` / variantes truncadas
-- `storico_recente_*`, `ecente_*`) de CAIXA e BB, lidos de
-- staging/dados_historicos/ via MinIO/DuckDB.
--
-- Responsabilidade desta camada (ver models/docs/arquitetura-medalhao-mcid.md):
--   * uma linha por linha de origem (SEM deduplicação);
--   * colunas da fonte preservadas como vieram (union_by_name entre CAIXA/BB);
--   * tipos genéricos (texto) — a coerção fica na silver;
--   * agrega os diversos meses (dt_referencia derivada do NOME DO ARQUIVO);
--   * carrega metadados de auditoria (source_file, dt_ingest, hash_linha) e os
--     metadados de tratamento embutidos (source_table, content_hash,
--     report_date, institution, profile).
--
-- NÃO entram aqui: os fluxos de entrega por evento (`o_recente_*_entregas` e
-- `*_entrega_da_unidade_af_b`) — filtrados por '%entrega%'. O reloginho usa o
-- acumulado `uh_entregues` do próprio snapshot (decisão D6 da #130); os fluxos
-- de evento são fonte de um modelo futuro separado.
--
-- Target obrigatório: staging_duckdb (gating em dbt_project.yml).

with

fonte as (
    select
        *,
        filename as source_file,
        strptime(
            regexp_extract(
                regexp_replace(filename, '(20\d{2})_(\d{2})', '\1\2'),
                '(\d{6})',
                1
            ),
            '%Y%m'
        )::date as dt_referencia,
        case
            when lower(filename) like '%af_bb%' then 'BB'
            when lower(filename) like '%af_caixa%' then 'CAIXA'
        end as agente_arquivo,
        case
            when lower(filename) like '%correcao%' then 3
            when regexp_matches(lower(filename), 'vs[0-9]+') then 2
            else 1
        end as prioridade_reentrega,
        current_timestamp as dt_ingest
    from {{ read_minio_staging_parquet_series(
        'dados_historicos/*ecente_*snh_pmcmv_dados_prioritarios_af_*.parquet'
    ) }}
    where lower(filename) not like '%entrega%'
)

select
    *,
    md5(concat_ws(
        '|',
        coalesce(agente_financeiro::text, agente_arquivo, ''),
        coalesce(apf::text, ''),
        coalesce(dt_referencia::text, ''),
        coalesce(modalidade::text, ''),
        coalesce(uh_contratadas::text, ''),
        coalesce(uh_entregues::text, ''),
        coalesce(uh_vigentes::text, '')
    )) as hash_linha
from fonte
