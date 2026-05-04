{{ config(materialized='table') }}

WITH source AS (
    SELECT *
    FROM {{ source('__dados_brutos', 'novo_mcmv_far_contratacao') }}
)

SELECT
    -- Identificação
    nu_apf,
    no_identificacao_proposta,
    no_nome_empreendimento,
    no_empreendimento_enquadramento,
    
    -- Tomador
    no_tomador,
    nu_cnpj_tomador,
    
    -- Proponente
    no_proponente,
    nu_cnpj_proponente,
    co_tipo_de_proponente,
    
    -- Localização
    no_logradouro_empreendimento,
    no_bairro,
    co_cep,
    no_municipio,
    no_uf,
    no_regiao,
    co_municipio_ibge,
    
    -- Características
    co_tipo_edificacao,
    co_originacao_empreendimento,
    co_tipo_de_demanda,
    co_tipo_demanda_emp_enquadramento,
    
    -- Unidades Habitacionais
    {{ parse_integer_value('nu_qt_uh_empreendimento') }} AS nu_qt_uh_empreendimento,
    {{ parse_integer_value('nu_qt_uh_empreendimento_enquadramento') }} AS nu_qt_uh_empreendimento_enquadramento,
    
    -- Valores Contratados
    {{ parse_financial_value('vr_empreendimento_far') }} AS vr_empreendimento_far,
    {{ parse_financial_value('vr_total_contrapartidas') }} AS vr_total_contrapartidas,
    
    -- Terreno / Regularidade
    ic_terreno_doado,
    co_qualificacao_terreno,
    ic_imovel_livre_onus,
    ic_normas_urbanisticas_conforme,
    
    -- Auditoria
    arquivo_de_origem,
    criado_em
FROM source
