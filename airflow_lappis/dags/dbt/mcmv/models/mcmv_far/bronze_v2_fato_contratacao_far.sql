{{ config(materialized='table') }}

WITH source AS (
    SELECT *
    FROM {{ source('__dados_brutos', 'novo_mcmv_far_contratacao') }}
)

SELECT
    -- Propostas / Contratações
    dt_recebimento_apta_gfar,
    dt_protocolo,
    hr_protocolo,
    no_agente_financeiro,
    no_identificacao_proposta,
    nu_apf,
    
    -- Situação da Proposta
    ic_sem_meta_uf,
    ic_dados_alterados_pos_contratacao,
    no_identificacao_proposta_substituida,
    
    -- Alterações
    no_campo_alterado_enquadramento,
    no_campo_alterado_contratacao,
    
    -- Valores
    {{ parse_financial_value('vr_empreendimento_far') }} AS vr_empreendimento_far,
    {{ parse_financial_value('vr_total_contrapartidas') }} AS vr_total_contrapartidas,
    
    -- Contrapartidas
    {{ parse_financial_value('vr_contrapartida_outras') }} AS vr_contrapartida_outras,
    {{ parse_financial_value('vr_contrapartida_estado_infra') }} AS vr_contrapartida_estado_infra,
    {{ parse_financial_value('vr_contrapartida_estado_terreno') }} AS vr_contrapartida_estado_terreno,
    {{ parse_financial_value('vr_contrapartida_complementar_estado') }} AS vr_contrapartida_complementar_estado,
    {{ parse_financial_value('vr_contrapartida_municipio_infra') }} AS vr_contrapartida_municipio_infra,
    {{ parse_financial_value('vr_contrapartida_municipio_terreno') }} AS vr_contrapartida_municipio_terreno,
    {{ parse_financial_value('vr_contrapartida_complementar_municipio') }} AS vr_contrapartida_complementar_municipio,
    
    -- Movimento
    dt_movimento,
    hr_movimento
FROM source
