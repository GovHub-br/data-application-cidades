{{ config(materialized="table") }}

-- Bronze: Trabalho Social Mensal — acompanhamento do TTS (Entidades)
-- Fonte: novo_mcmv_fds_trabalho_social_mensal
-- Saída: dados de trabalho social tipados
with
    ts_raw as (
        select
            -- Identificação
            {{ normalize_apf('nu_apf') }} as apf,

            -- Situação do trabalho social
            {{ parse_int('co_situacao_trabalho_social') }} as co_situacao_trabalho_social,

            -- Datas do ciclo de TS
            {{ parse_date_br('dt_aprovacao_pts') }} as dt_aprovacao_pts,
            {{ parse_date_br('dt_assinatura_convenio') }} as dt_assinatura_convenio,
            {{ parse_date_br('dt_termino_convenio') }} as dt_termino_convenio,
            {{ parse_date_br('dt_primeiro_relatorio') }} as dt_primeiro_relatorio,
            {{ parse_date_br('dt_ultimo_relatorio') }} as dt_ultimo_relatorio,

            -- Percentual de execução do TS
            {{ parse_numeric('pc_execucao_ts', 'numeric(6, 2)') }} as pct_execucao_ts,

            -- Portaria
            nullif(trim(nu_portaria_ts), '') as nu_portaria_ts,
            {{ parse_date_br('dt_publ_portaria_ts') }} as dt_publicacao_portaria,

            -- Referência temporal
            {{ parse_date_br('dh_movimento') }} as dt_movimento,

            -- Colunas técnicas (padrão medalhão §6)
            coalesce(nullif(trim(arquivo_de_origem), ''), _source_file) as source_file,
            coalesce(try_cast(_ingested_at as timestamp), current_timestamp) as dt_ingest,
            _source_hash as hash_linha,
            {{ hist_dt_referencia_from_filename('arquivo_de_origem') }} as dt_referencia

        from {{ source("mcmv_staging", "novo_mcmv_fds_trabalho_social_mensal") }}
    )

select *
from ts_raw
