{{ config(materialized="table") }}

-- Silver: Trabalho Social PNHR CAIXA
-- Fonte: bronze.bronze_trabalho_social_caixa (parquet da staging/ carregado pelo staging_para_bronze.py)
-- Saída: dados de trabalho social da Caixa limpos e tipados

with
    ts_caixa_raw as (
        select
            -- Identificadores
            {{ target.schema }}.normalize_apf(contrato) as apf,
            nullif(trim({{ target.schema }}.corrigir_mojibake(contrato)), '') as contrato,
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
            {{ parse_financial_value('vr_global_ts') }} as vr_global_ts,
            {{ parse_financial_value('vr_desembolsado') }} as vr_desembolsado_ts,
            {{ parse_financial_value('vr_a_desembolsar_nao_concluidos') }} as vr_a_desembolsar_nao_concluidos,
            {{ parse_financial_value('vr_nao_desembolsado_concluidos') }} as vr_nao_desembolsado_concluidos,

            -- Execução e Status
            {{ parse_numeric('percentual_execucao_ts', 'numeric(6, 2)') }} as percentual_execucao_ts,
            {{ parse_numeric('percentual_obra', 'numeric(6, 2)') }} as percentual_obra,
            nullif(trim({{ target.schema }}.corrigir_mojibake(situacao_ts)), '') as situacao_ts,
            nullif(trim({{ target.schema }}.corrigir_mojibake(motivo_situacao_ts_atrasado_paralisado)), '') as motivo_situacao_ts,

            -- Outros Metadados
            nullif(trim({{ target.schema }}.corrigir_mojibake(portaria_adotada)), '') as portaria_adotada,
            nullif(trim({{ target.schema }}.corrigir_mojibake(instrumento_de_planejamento)), '') as instrumento_planejamento,
            nullif(trim({{ target.schema }}.corrigir_mojibake(natureza_execucao)), '') as natureza_execucao,

            -- Datas
            {{ target.schema }}.parse_date_br(data_da_contratacao) as dt_contratacao,
            -- A base da CAIXA não informa a data do primeiro relatório (só a do
            -- BB tem). Mantido nulo para as duas silver terem o mesmo formato.
            null::date as dt_primeiro_relatorio,
            {{ target.schema }}.parse_date_br(dt_entrega) as dt_entrega,
            {{ target.schema }}.parse_date_br(dt_ultimo_avt) as dt_ultimo_avt,
            {{ target.schema }}.parse_date_br(dt_avf) as dt_avf,

            -- Linhagem da bronze do lake
            _source_file as arquivo_de_origem,
            nullif(trim({{ target.schema }}.corrigir_mojibake(_ingested_at)), '')::timestamp as criado_em

        from {{ source("bronze_rural", "bronze_trabalho_social_caixa") }}
    )

select *
from ts_caixa_raw
