{{ config(materialized="table") }}

-- Bronze: Trabalho Social Mensal — acompanhamento do TTS (Entidades)
-- Fonte: novo_mcmv_fds_trabalho_social_mensal
-- Saída: dados de trabalho social tipados

with
    ts_raw as (
        select
            -- Identificação
            {{ target.schema }}.normalize_apf(nu_apf) as apf,

            -- Situação do trabalho social
            {{ parse_int('co_situacao_trabalho_social') }} as co_situacao_trabalho_social,

            -- Datas do ciclo de TS
            {{ target.schema }}.parse_date_br(dt_aprovacao_pts) as dt_aprovacao_pts,
            {{ target.schema }}.parse_date_br(dt_assinatura_convenio) as dt_assinatura_convenio,
            {{ target.schema }}.parse_date_br(dt_termino_convenio) as dt_termino_convenio,
            {{ target.schema }}.parse_date_br(dt_primeiro_relatorio) as dt_primeiro_relatorio,
            {{ target.schema }}.parse_date_br(dt_ultimo_relatorio) as dt_ultimo_relatorio,

            -- Percentual de execução do TS
            {{ parse_numeric('pc_execucao_ts', 'numeric(6, 2)') }} as pct_execucao_ts,

            -- Portaria
            nullif(trim(nu_portaria_ts), '') as nu_portaria_ts,
            {{ target.schema }}.parse_date_br(dt_publ_portaria_ts) as dt_publicacao_portaria,

            -- Referência temporal
            {{ target.schema }}.parse_date_br(dh_movimento) as dt_movimento,

            -- Metadados
            arquivo_de_origem,
            criado_em

        from {{ source("raw", "novo_mcmv_fds_trabalho_social_mensal") }}
    )

select *
from ts_raw
