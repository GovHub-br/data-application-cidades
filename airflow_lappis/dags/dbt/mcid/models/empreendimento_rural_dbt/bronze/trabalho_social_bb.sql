{{ config(materialized="table") }}

-- Bronze: Trabalho Social PNHR Banco do Brasil
-- Fonte: base_trabalho_social_pnhr_bb
-- Saída: dados de trabalho social do BB limpos e tipados

with
    ts_bb_raw as (
        select
            -- Identificadores
            {{ target.schema }}.normalize_apf(contrato_registro_ao) as apf,
            nullif(trim(contrato_registro_ao), '') as contrato,
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
            {{ parse_financial_value('vr_total_ts') }} as vr_global_ts,
            {{ parse_financial_value('vr_desembolsado_ts') }} as vr_desembolsado_ts,
            {{ parse_financial_value('vr_a_desembolsar_ts') }} as vr_a_desembolsar_nao_concluidos,

            -- Execução e Status
            {{ parse_numeric('percentual_execucao_ts', 'numeric(6, 2)') }} as percentual_execucao_ts,
            {{ parse_numeric('percentual_obra', 'numeric(6, 2)') }} as percentual_obra,
            nullif(trim(situacao_ts), '') as situacao_ts,
            nullif(trim(motivo_situacao_ts_atrasado_paralisado), '') as motivo_situacao_ts,

            -- Outros Metadados
            nullif(trim(portaria_ts_utilizada), '') as portaria_adotada,
            nullif(trim(instrumento_de_planejamento), '') as instrumento_planejamento,
            nullif(trim(forma_natureza_de_execucao_direta_indireta_mista_pelo_af), '') as natureza_execucao,

            -- Datas
            {{ target.schema }}.parse_date_br(data_contratacao_empreendimento) as dt_contratacao,
            {{ target.schema }}.parse_date_br(data_primeiro_relatorio) as dt_primeiro_relatorio,
            {{ target.schema }}.parse_date_br(data_ultimo_relatorio) as dt_ultimo_relatorio,
            {{ target.schema }}.parse_date_br(dt_entrega_data_da_assinatura_do_primeiro_contrato_de_pessoa_fi) as dt_entrega,

            -- Metadados
            arquivo_de_origem,
            criado_em

        from {{ source("raw", "base_trabalho_social_pnhr_bb") }}
    )

select *
from ts_bb_raw
