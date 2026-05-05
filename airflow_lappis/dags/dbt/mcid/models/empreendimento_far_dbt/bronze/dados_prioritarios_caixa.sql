{{ config(materialized="table") }}

-- Bronze: Dados Prioritários CAIXA — snapshot consolidado da CAIXA
-- Fonte: dados_prioritarios_recebidos_caixa_empreendimentos (4.443 FAR, ~15k total)
-- Saída: apenas empreendimentos FAR, com tipagem correta
-- Nota: filtra apenas modalidade FAR nesta camada bronze

with
    prioritarios_raw as (
        select
            -- Identificação
            nullif(trim(apf), '') as apf,
            nullif(trim(agente_financeiro), '') as agente_financeiro,
            nullif(trim(nome_empreendimento), '') as empreendimento_nome,

            -- Localização
            nullif(trim(uf), '') as uf,
            nullif(trim(municipio), '') as municipio,
            nullif(trim(codigo_ibge_do_municipio), '') as cod_ibge,

            -- Situação
            nullif(trim(modalidade), '') as modalidade,
            nullif(trim(situacao_do_empreendimento), '') as situacao,
            nullif(trim(detalhamento_da_situacao_do_empreendimento), '') as situacao_detalhamento,

            -- Execução física
            {{ parse_numeric('percentual_exec', 'numeric(6, 2)') }} as pct_execucao,

            -- Valores financeiros (formato "34679700" ou "33275351,54")
            {{ parse_financial_value('valor_contratado') }} as valor_contratado,
            {{ parse_financial_value('valor_aporte_adicional') }} as valor_aporte_adicional,
            {{ parse_financial_value('valor_desembolsado') }} as valor_desembolsado,

            -- UHs
            {{ parse_int('uh_contratadas') }} as uh_contratadas,
            {{ parse_int('uh_entregues') }} as uh_entregues,
            {{ parse_int('uh_vigentes') }} as uh_vigentes,

            -- Datas
            case
                when data_de_contratacao is null or trim(data_de_contratacao) = '' then null
                when data_de_contratacao ~ '^\d{4}-\d{2}-\d{2}' then data_de_contratacao::date
                else {{ target.schema }}.parse_date_br(data_de_contratacao)
            end as dt_contratacao,
            case
                when data_da_previsao_da_entrega is null or trim(data_da_previsao_da_entrega) = '' then null
                when data_da_previsao_da_entrega ~ '^\d{4}-\d{2}-\d{2}' then data_da_previsao_da_entrega::date
                else {{ target.schema }}.parse_date_br(data_da_previsao_da_entrega)
            end as dt_previsao_entrega,
            case
                when data_de_movimento is null or trim(data_de_movimento) = '' then null
                when data_de_movimento ~ '^\d{4}-\d{2}-\d{2}' then data_de_movimento::date
                else {{ target.schema }}.parse_date_br(data_de_movimento)
            end as dt_movimento,

            -- Endereço
            nullif(trim(logradouro_do_imovel), '') as logradouro,
            nullif(trim(bairro_do_imovel), '') as bairro,
            nullif(trim(cep_do_imovel), '') as cep,

            -- Coordenadas (já em formato decimal)
            {{ parse_numeric('latitude_do_imovel', 'numeric(12, 8)') }} as latitude,
            {{ parse_numeric('longitude_do_imovel', 'numeric(12, 8)') }} as longitude,

            -- Desembolso do ano
            {{ parse_financial_value('valor_desembolsado_do_ano_de_referencia') }} as valor_desembolsado_ano,

            -- UHs por período
            {{ parse_int('unidades_habitacionais_a_serem_entregues') }} as uh_a_entregar,
            {{ parse_int('quantidade_de_uhs_distratadas') }} as uh_distratadas,

            -- Observações
            nullif(trim(observacoes), '') as observacoes,

            -- Metadados
            arquivo_de_origem,
            criado_em

        from {{ source("raw", "dados_prioritarios_recebidos_caixa_empreendimentos") }}
        where trim(modalidade) = 'FAR'
    )

select *
from prioritarios_raw
