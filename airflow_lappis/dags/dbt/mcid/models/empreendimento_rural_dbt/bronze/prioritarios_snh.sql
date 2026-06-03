{{ config(materialized="table") }}

-- Bronze: Dados Prioritários SNH (Rural)
-- Fonte: dados_prioritarios_disponibilizados_snh_empreendimentos
-- Saída: dados prioritários gerais limpos, tipados e filtrados para modalidade Rural

with
    snh_raw as (
        select
            -- Identificação
            {{ target.schema }}.normalize_apf(codigo_da_operacao_no_agente_financeiro) as apf,
            nullif(trim(identificador_da_operacao_na_snh), '') as id_operacao_snh,
            nullif(trim(codigo_da_operacao_no_agente_financeiro), '') as cod_operacao_agente,
            nullif(trim(nome_do_agente_financeiro), '') as agente_financeiro,
            nullif(trim(nome_do_empreendimento), '') as empreendimento_nome,
            nullif(trim(modalidade), '') as modalidade,

            -- Construtora / Entidade
            nullif(trim(nome_da_construtora_entidade), '') as construtora_nome,
            nullif(regexp_replace(trim(nome_da_construtora_entidade_2_cnpj_da_construtora_entidade), '[^0-9]', '', 'g'), '') as construtora_cnpj,

            -- Localização
            nullif(trim(municipio), '') as municipio,
            nullif(trim(sigla_da_uf), '') as uf,
            nullif(trim(nome_da_uf), '') as estado_nome,
            nullif(trim(nome_da_regiao), '') as regiao,
            nullif(trim(codigo_ibge_do_municipio), '') as cod_ibge,

            -- Endereço
            nullif(trim(logradouro), '') as logradouro,
            nullif(trim(numero_do_imovel), '') as numero_imovel,
            nullif(trim(complemento_do_logradouro), '') as complemento_logradouro,
            nullif(trim(bairro), '') as bairro,
            nullif(trim(cep), '') as cep,

            -- Coordenadas
            {{ parse_numeric('latitude', 'numeric(12, 8)') }} as latitude,
            {{ parse_numeric('longitude', 'numeric(12, 8)') }} as longitude,

            -- Situação e Fase
            nullif(trim(situacao_do_empreendimento), '') as situacao,
            nullif(trim(detalhamento_da_situacao_do_empreendimento), '') as situacao_detalhamento,
            nullif(trim(situacao_da_empreendimento_agrupada), '') as situacao_agrupada,
            case when trim(mudou_de_fase) = 'Sim' then true else false end as mudou_de_fase,
            nullif(trim(apf_da_fase_obra), '') as apf_fase_obra,
            case when trim(novo_mcmv_sim_nao) = 'Sim' then true else false end as ic_novo_mcmv,

            -- Execução Física (%)
            {{ parse_numeric('percentual_da_obra', 'numeric(6, 2)') }} as percentual_execucao_fisica,

            -- UHs
            {{ parse_int('unidades_contratadas') }} as uh_contratadas,
            {{ parse_int('unidades_entregues') }} as uh_entregues,
            {{ parse_int('unidades_vigentes') }} as uh_vigentes,
            {{ parse_int('unidades_distratadas') }} as uh_distratadas,
            {{ parse_int('unidades_habitacionais_a_serem_entregues') }} as uh_a_entregar,

            -- Valores
            {{ parse_financial_value('valor_contratado_original') }} as valor_contratado_original,
            {{ parse_financial_value('valor_do_aporte_adicional') }} as valor_aporte_adicional,
            {{ parse_financial_value('valor_contratado_total') }} as valor_contratado,
            {{ parse_financial_value('valor_desembolsado_total') }} as valor_desembolsado,

            -- Datas
            case
                when data_da_contratacao is null or trim(data_da_contratacao) = '' then null
                when data_da_contratacao ~ '^\d{4}-\d{2}-\d{2}' then data_da_contratacao::date
                else {{ target.schema }}.parse_date_br(data_da_contratacao)
            end as dt_contratacao,
            case
                when data_de_previsao_de_termino is null or trim(data_de_previsao_de_termino) = '' then null
                when data_de_previsao_de_termino ~ '^\d{4}-\d{2}-\d{2}' then data_de_previsao_de_termino::date
                else {{ target.schema }}.parse_date_br(data_de_previsao_de_termino)
            end as dt_previsao_entrega,
            case
                when data_do_termino is null or trim(data_do_termino) = '' then null
                when data_do_termino ~ '^\d{4}-\d{2}-\d{2}' then data_do_termino::date
                else {{ target.schema }}.parse_date_br(data_do_termino)
            end as dt_termino,
            case
                when data_de_referencia is null or trim(data_de_referencia) = '' then null
                when data_de_referencia ~ '^\d{4}-\d{2}-\d{2}' then data_de_referencia::date
                else {{ target.schema }}.parse_date_br(data_de_referencia)
            end as dt_referencia,

            -- Metadados
            arquivo_de_origem,
            criado_em

        from {{ source("raw", "dados_prioritarios_disponibilizados_snh_empreendimentos") }}
        where trim(upper(modalidade)) = 'RURAL'
    )

select *
from snh_raw
