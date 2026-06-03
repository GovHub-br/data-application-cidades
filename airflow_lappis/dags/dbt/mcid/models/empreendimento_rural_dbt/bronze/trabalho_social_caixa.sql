{{ config(materialized="table") }}

-- Bronze: Trabalho Social PNHR CAIXA
-- Fonte: base_trabalho_social_pnhr_rural_caixa
-- Saída: dados de trabalho social da Caixa limpos e tipados

with
    ts_caixa_raw as (
        select
            -- Identificadores
            {{ target.schema }}.normalize_apf(contrato) as apf,
            nullif(trim(contrato), '') as contrato,
            nullif(trim(recurso), '') as recurso,
            nullif(trim(nome_empreendimento), '') as empreendimento_nome,
            
            -- Localização
            nullif(trim(municipio), '') as municipio,
            nullif(trim(uf), '') as uf,

            -- Quantidades e Tipologia
            {{ parse_int('uh') }} as qt_uh,
            nullif(trim(tipologia), '') as tipologia,
            nullif(trim(fase_mcmv), '') as fase_mcmv,

            -- Valores
            {{ parse_financial_value('vr_global_ts') }} as vr_global_ts,
            {{ parse_financial_value('vr_desembolsado') }} as vr_desembolsado_ts,
            {{ parse_financial_value('vr_a_desembolsar_nao_concluidos') }} as vr_a_desembolsar_nao_concluidos,
            {{ parse_financial_value('vr_nao_desembolsado_concluidos') }} as vr_nao_desembolsado_concluidos,

            -- Execução e Status
            {{ parse_numeric('percentual_execucao_ts', 'numeric(6, 2)') }} as percentual_execucao_ts,
            {{ parse_numeric('percentual_obra', 'numeric(6, 2)') }} as percentual_obra,
            nullif(trim(situacao_ts), '') as situacao_ts,
            nullif(trim(motivo_situacao_ts_atrasado_paralisado), '') as motivo_situacao_ts,

            -- Outros Metadados
            nullif(trim(portaria_adotada), '') as portaria_adotada,
            nullif(trim(instrumento_de_planejamento), '') as instrumento_planejamento,
            nullif(trim(natureza_execucao), '') as natureza_execucao,

            -- Datas
            {{ target.schema }}.parse_date_br(data_da_contratacao) as dt_contratacao,
            {{ target.schema }}.parse_date_br(data_primeiro_relatorio) as dt_primeiro_relatorio,
            {{ target.schema }}.parse_date_br(dt_entrega) as dt_entrega,
            {{ target.schema }}.parse_date_br(dt_ultimo_avt) as dt_ultimo_avt,
            {{ target.schema }}.parse_date_br(dt_avf) as dt_avf,

            -- Metadados
            arquivo_de_origem,
            criado_em

        from {{ source("raw", "base_trabalho_social_pnhr_rural_caixa") }}
    )

select *
from ts_caixa_raw
