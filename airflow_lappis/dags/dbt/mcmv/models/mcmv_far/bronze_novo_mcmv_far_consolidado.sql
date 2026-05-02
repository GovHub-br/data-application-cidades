select

    -- IDENTIFICAÇÃO
    cast(nu_apf as varchar) as apf,
    cast(nu_operacao_af as varchar) as cod_operacao_af,

    cast(no_identificacao_proposta as varchar) as no_identificacao_proposta,
    cast(no_identificacao_proposta_substituida as varchar) as no_identificacao_proposta_substituida,

    -- LOCALIZAÇÃO
    cast(upper(trim(no_uf)) as varchar) as uf,
    cast(upper(trim(no_municipio)) as varchar) as municipio,
    cast(co_municipio_ibge as varchar) as co_municipio_ibge,
    cast(upper(trim(no_regiao)) as varchar) as regiao,

    -- EMPREENDIMENTO
    cast(upper(trim(no_nome_empreendimento)) as varchar) as nome_empreendimento,
    cast(upper(trim(no_empreendimento_contratacao)) as varchar) as nome_empreendimento_contratacao,

    -- STATUS / FLUXO
    cast(co_fase as varchar) as co_fase,
    cast(co_etapa as varchar) as co_etapa,
    cast(co_status as varchar) as co_status,

    cast(co_situacao_proposta_protocolizada as varchar) as co_situacao_proposta_protocolizada,
    cast(co_situacao_proposta_contratada as varchar) as co_situacao_proposta_contratada,
    cast(co_justificativa_nao_enquadramento_af as varchar) as co_justificativa_nao_enquadramento_af,

    cast(ic_enquadramento_proposta as varchar) as ic_enquadramento_proposta,

    -- DATAS PROCESSO
    to_date(dt_protocolo, 'DD/MM/YYYY') as dt_protocolo,
    to_date(dt_recebimento_gfar, 'DD/MM/YYYY') as dt_recebimento_gfar,
    to_date(dt_recebimento_apta_gfar, 'DD/MM/YYYY') as dt_recebimento_apta_gfar,
    to_date(dt_recurso, 'DD/MM/YYYY') as dt_recurso,
    to_date(dt_vencimento_portaria_contratacao, 'DD/MM/YYYY') as dt_vencimento_portaria_contratacao,

    -- PORTARIA
    cast(nu_portaria_contratacao as varchar) as nu_portaria_contratacao,

    -- UHs
    cast(nu_qt_uh_empreendimento as integer) as uh_proposta,
    cast(nu_qt_uh_empreendimento_contratacao as integer) as uh_contratadas,

    -- FINANCEIRO
    {{ parse_financial_value('vr_empreendimento_far') }} as vl_empreendimento_far,
    {{ parse_financial_value('vr_total_contrapartidas') }} as vl_total_contrapartidas,

    {{ parse_financial_value('vr_contrapartida_estado_infra') }} as vl_contrapartida_estado_infra,
    {{ parse_financial_value('vr_contrapartida_estado_terreno') }} as vl_contrapartida_estado_terreno,
    {{ parse_financial_value('vr_contrapartida_municipio_infra') }} as vl_contrapartida_municipio_infra,
    {{ parse_financial_value('vr_contrapartida_municipio_terreno') }} as vl_contrapartida_municipio_terreno,

    -- CRITÉRIOS / ENQUADRAMENTO
    cast(co_tipo_de_demanda as varchar) as co_tipo_de_demanda,
    cast(co_tipo_de_proponente as varchar) as co_tipo_de_proponente,
    cast(co_tipo_edificacao as varchar) as co_tipo_edificacao,
    cast(co_qualificacao_terreno as varchar) as co_qualificacao_terreno,

    cast(ic_terreno_doado as varchar) as ic_terreno_doado,
    cast(ic_contrapartida_outras as varchar) as ic_contrapartida_outras,
    cast(ic_terreno_area_urbana_consolidada as varchar) as ic_terreno_area_urbana_consolidada,
    cast(ic_terreno_area_urbana_expansao as varchar) as ic_terreno_area_urbana_expansao,

    cast(ic_energia_eletrica_iluminacao_publica as varchar) as ic_energia_eletrica_iluminacao_publica,
    cast(ic_agua_potavel as varchar) as ic_agua_potavel,
    cast(ic_rede_esgoto_coleta_lixo as varchar) as ic_rede_esgoto_coleta_lixo,
    cast(ic_via_pavimentada as varchar) as ic_via_pavimentada,
    cast(ic_drenagem_pluvial as varchar) as ic_drenagem_pluvial

from {{ source('__dados_brutos', 'novo_mcmv_far_consolidado') }}