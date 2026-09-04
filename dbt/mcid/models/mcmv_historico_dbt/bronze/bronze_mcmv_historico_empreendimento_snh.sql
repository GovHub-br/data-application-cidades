{{ config(materialized="table") }}

-- BRONZE — série mensal SNH de "dados prioritários" por empreendimento MCMV,
-- cópia fiel. Fonte mais rica de história por empreendimento (2024-06+), com a
-- coluna `modalidade` (FAR / Entidades / Rural) que discrimina a frente.
--
-- Empilha, SEM regra de negócio e SEM deduplicação, todos os snapshots mensais
-- `historico_recente_*` / variantes truncadas (`storico_recente_*`, `ecente_*`)
-- de CAIXA e BB, lidos de staging/dados_historicos/ via MinIO/DuckDB.
--
-- NÃO entram aqui os fluxos de ENTREGA por evento (`*entrega*`) — filtrados. O
-- reloginho usa o acumulado `uh_entregues` do próprio snapshot (decisão D6 da
-- #130); os fluxos de evento são fonte de bronze_reloginho_snh_entregas_evento.
--
-- Esta bronze é compartilhada: consumida pela silver histórica por frente
-- (silver_mcmv_historico_empreendimento_far/_fds/_rural) E pelo reloginho
-- (bronze_reloginho_snh_serie_mensal passa a ref() esta tabela). Por isso
-- preserva os campos derivados que o reloginho usa hoje: agente_arquivo,
-- prioridade_reentrega, dt_referencia.
--
-- Responsabilidade da camada: uma linha por linha de origem; colunas da fonte
-- preservadas (union_by_name entre CAIXA/BB); tipos genéricos (texto);
-- dt_referencia derivada do NOME DO ARQUIVO; auditoria (source_file, dt_ingest,
-- hash_linha).
--
-- Target obrigatório: staging_duckdb (gating em dbt_project.yml).
with

    fonte as (
        select
            *,
            filename as source_file,
            strptime(
                regexp_extract(
                    regexp_replace(filename, '(20\d{2})_(\d{2})', '\1\2'), '(\d{6})', 1
                ),
                '%Y%m'
            )::date as dt_referencia,
            case
                when lower(filename) like '%af_bb%'
                then 'BB'
                when lower(filename) like '%af_caixa%'
                then 'CAIXA'
            end as agente_arquivo,
            case
                when lower(filename) like '%correcao%'
                then 3
                when regexp_matches(lower(filename), 'vs[0-9]+')
                then 2
                else 1
            end as prioridade_reentrega,
            current_timestamp as dt_ingest
        from
            {{ read_minio_staging_parquet_series(
        'dados_historicos/*ecente_*snh_pmcmv_dados_prioritarios_af_*.parquet'
    ) }}
        where lower(filename) not like '%entrega%'
    )

select
    *,
    md5(
        concat_ws(
            '|',
            coalesce(agente_financeiro::text, agente_arquivo, ''),
            coalesce(apf::text, ''),
            coalesce(dt_referencia::text, ''),
            coalesce(modalidade::text, ''),
            coalesce(uh_contratadas::text, ''),
            coalesce(uh_entregues::text, ''),
            coalesce(uh_vigentes::text, '')
        )
    ) as hash_linha
from fonte
