{{ config(materialized="table") }}

-- Bronze: Trabalho Técnico Social FAR — CAIXA + BB empilhados (union all)
-- Fontes: mcmv_staging.base_trabalho_social_far_caixa / _far_bb (staging/sharepoint)
-- Cópia fiel com contrato comum: os dois agentes têm nomes de coluna distintos,
-- normalizados aqui para um shape único + coluna `agente`. Sem chave de APF
-- (grão = contrato/recurso); é fonte standalone, não entra na silver por APF.

with
    caixa as (
        select
            'CAIXA' as agente,
            nullif(trim(recurso), '') as recurso,
            nullif(trim(contrato), '') as contrato,
            nullif(trim(municipio), '') as municipio,
            nullif(trim(uf), '') as uf,
            nullif(trim(nome_empreendimento), '') as empreendimento_nome,
            {{ parse_int('uh') }} as uh,
            nullif(trim(tipologia), '') as tipologia,
            nullif(trim(fase_mcmv), '') as fase_mcmv,
            {{ parse_date_br('data_da_contratacao') }} as dt_contratacao,
            {{ parse_hist_numeric('vr_global_ts') }} as vr_ts_global,
            {{ parse_hist_numeric('vr_desembolsado') }} as vr_ts_desembolsado,
            {{ parse_numeric('percentual_execucao_ts', 'numeric(6, 2)') }} as pct_execucao_ts,
            {{ parse_numeric('percentual_obra', 'numeric(6, 2)') }} as pct_obra,
            nullif(trim(portaria_adotada), '') as portaria_ts,
            nullif(trim(instrumento_de_planejamento), '') as instrumento_planejamento,
            {{ parse_date_br('data_entrega') }} as dt_entrega,
            nullif(trim(situacao_ts), '') as situacao_ts,
            nullif(trim(motivo_situacao_ts_atrasado_paralisado), '') as motivo_situacao_ts,
            coalesce(nullif(trim(arquivo_de_origem), ''), _source_file) as source_file,
            coalesce(try_cast(_ingested_at as timestamp), current_timestamp) as dt_ingest,
            _source_hash as hash_linha,
            {{ hist_dt_referencia_from_filename('arquivo_de_origem') }} as dt_referencia
        from {{ source("mcmv_staging", "base_trabalho_social_far_caixa") }}
    ),

    bb as (
        select
            'BB' as agente,
            nullif(trim(recurso), '') as recurso,
            nullif(trim(contrato_registro_ao), '') as contrato,
            nullif(trim(municipio), '') as municipio,
            nullif(trim(uf), '') as uf,
            nullif(trim(nome_empreendimento), '') as empreendimento_nome,
            {{ parse_int('uh') }} as uh,
            nullif(trim(tipologia), '') as tipologia,
            nullif(trim(fase_mcmv), '') as fase_mcmv,
            {{ parse_date_br('data_contratacao_empreendimento') }} as dt_contratacao,
            {{ parse_hist_numeric('vr_total_ts') }} as vr_ts_global,
            {{ parse_hist_numeric('vr_desembolsado_ts') }} as vr_ts_desembolsado,
            {{ parse_numeric('percentual_execucao_ts', 'numeric(6, 2)') }} as pct_execucao_ts,
            {{ parse_numeric('percentual_obra', 'numeric(6, 2)') }} as pct_obra,
            nullif(trim(portaria_ts_utilizada), '') as portaria_ts,
            nullif(trim(instrumento_de_planejamento), '') as instrumento_planejamento,
            {{ parse_date_br('dt_entrega') }} as dt_entrega,
            nullif(trim(situacao_ts), '') as situacao_ts,
            nullif(trim(motivo_situacao_ts_atrasado_paralisado), '') as motivo_situacao_ts,
            coalesce(nullif(trim(arquivo_de_origem), ''), _source_file) as source_file,
            coalesce(try_cast(_ingested_at as timestamp), current_timestamp) as dt_ingest,
            _source_hash as hash_linha,
            {{ hist_dt_referencia_from_filename('arquivo_de_origem') }} as dt_referencia
        from {{ source("mcmv_staging", "base_trabalho_social_far_bb") }}
    )

select * from caixa
union all by name
select * from bb
