{{ config(materialized="table") }}

-- Silver: Trabalho Social PNHR Banco do Brasil
-- Fonte: bronze.bronze_trabalho_social_bb (parquet da staging/ carregado pelo staging_para_bronze.py)
-- Saída: dados de trabalho social do BB limpos e tipados

with
    ts_bb_raw as (
        select
            -- Identificadores
            {{ target.schema }}.normalize_apf(contrato_registro_ao) as apf,
            nullif(trim({{ target.schema }}.corrigir_mojibake(contrato_registro_ao)), '') as contrato,
            nullif(trim({{ target.schema }}.corrigir_mojibake(recurso)), '') as recurso,
            nullif(trim({{ target.schema }}.corrigir_mojibake(nome_empreendimento)), '') as empreendimento_nome,

            -- Localização
            nullif(trim({{ target.schema }}.corrigir_mojibake(municipio)), '') as municipio,
            nullif(trim({{ target.schema }}.corrigir_mojibake(uf)), '') as uf,

            -- Quantidades e Tipologia
            {{ parse_int('uh') }} as qt_uh,
            nullif(trim({{ target.schema }}.corrigir_mojibake(tipologia)), '') as tipologia,
            nullif(trim({{ target.schema }}.corrigir_mojibake(fase_mcmv)), '') as fase_mcmv,

            -- Valores
            {{ parse_financial_value('vr_total_ts') }} as vr_global_ts,
            {{ parse_financial_value('vr_desembolsado_ts') }} as vr_desembolsado_ts,
            {{ parse_financial_value('vr_a_desembolsar_ts') }} as vr_a_desembolsar_nao_concluidos,

            -- Execução e Status
            {{ parse_numeric('percentual_execucao_ts', 'numeric(6, 2)') }} as percentual_execucao_ts,
            {{ parse_numeric('percentual_obra', 'numeric(6, 2)') }} as percentual_obra,
            nullif(trim({{ target.schema }}.corrigir_mojibake(situacao_ts)), '') as situacao_ts,
            nullif(trim({{ target.schema }}.corrigir_mojibake(motivo_situacao_ts_atrasado_paralisado)), '') as motivo_situacao_ts,

            -- Outros Metadados
            nullif(trim({{ target.schema }}.corrigir_mojibake(portaria_ts_utilizada)), '') as portaria_adotada,
            nullif(trim({{ target.schema }}.corrigir_mojibake(instrumento_de_planejamento)), '') as instrumento_planejamento,
            nullif(trim({{ target.schema }}.corrigir_mojibake(forma_natureza_de_execucao_direta_indireta_mista_pelo_af)), '') as natureza_execucao,

            -- Datas
            {{ target.schema }}.parse_date_br(data_contratacao_empreendimento) as dt_contratacao,
            {{ target.schema }}.parse_date_br(data_primeiro_relatorio) as dt_primeiro_relatorio,
            {{ target.schema }}.parse_date_br(data_ultimo_relatorio) as dt_ultimo_relatorio,
            {{ target.schema }}.parse_date_br(data_da_assinatura_do_primeiro_contrato_de_pessoa_fisica) as dt_entrega,

            -- Linhagem da bronze do lake
            _source_file as arquivo_de_origem,
            nullif(trim({{ target.schema }}.corrigir_mojibake(_ingested_at)), '')::timestamp as criado_em

        from {{ source("bronze_rural", "bronze_trabalho_social_bb") }}
    )

select *
from ts_bb_raw
