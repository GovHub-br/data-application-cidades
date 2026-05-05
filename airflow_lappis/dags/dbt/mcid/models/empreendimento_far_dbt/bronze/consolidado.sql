{{ config(materialized="table") }}

-- Bronze: Consolidado GFAR — propostas e dados de seleção/contratação
-- Fonte: novo_mcmv_far_consolidado (15.091 registros, 97 colunas text)
-- Saída: apenas colunas relevantes para a ficha do empreendimento, com tipagem correta

with
    consolidado_raw as (
        select
            -- Identificação da proposta
            trim(no_identificacao_proposta) as id_proposta,
            {{ target.schema }}.normalize_apf(nu_apf) as apf,
            nullif(trim(nu_operacao_af), '') as nu_operacao_af,

            -- Portaria e situação
            nullif(trim(nu_portaria_contratacao), '') as portaria_contratacao,
            {{ parse_int('co_situacao_proposta_contratada') }} as co_situacao_contratada,
            {{ parse_int('co_situacao_proposta_protocolizada') }} as co_situacao_protocolizada,

            -- Fase/etapa do fluxo GFAR
            {{ parse_int('co_fase') }} as co_fase,
            {{ parse_int('co_etapa') }} as co_etapa,
            {{ parse_int('co_status') }} as co_status,

            -- Proponente
            nullif(trim(no_proponente), '') as proponente_nome,
            nullif(regexp_replace(trim(nu_cnpj_proponente), '[^0-9]', '', 'g'), '') as proponente_cnpj,
            {{ parse_int('co_tipo_de_proponente') }} as co_tipo_proponente,

            -- Tomador
            nullif(trim(no_tomador), '') as tomador_nome,
            nullif(regexp_replace(trim(nu_cnpj_tomador), '[^0-9]', '', 'g'), '') as tomador_cnpj,

            -- Empreendimento
            coalesce(
                nullif(trim(no_empreendimento_contratacao), ''),
                nullif(trim(no_nome_empreendimento), '')
            ) as empreendimento_nome,
            nullif(trim(no_agente_financeiro), '') as agente_financeiro,

            -- Localização
            nullif(trim(no_municipio), '') as municipio,
            nullif(trim(no_uf), '') as uf,
            nullif(trim(no_regiao), '') as regiao,
            nullif(trim(co_municipio_ibge), '') as cod_ibge,

            -- Endereço
            nullif(trim(no_logradouro_empreendimento), '') as logradouro,
            nullif(trim(no_bairro), '') as bairro,
            nullif(trim(co_cep), '') as cep,

            -- Tipologia e UHs
            {{ parse_int('co_tipo_edificacao') }} as co_tipo_edificacao,
            coalesce(
                {{ parse_int('nu_qt_uh_empreendimento_contratacao') }},
                {{ parse_int('nu_qt_uh_empreendimento') }}
            ) as qt_uh,

            -- Demanda
            {{ parse_int('co_tipo_de_demanda') }} as co_tipo_demanda,
            {{ parse_int('co_originacao_empreendimento') }} as co_originacao,

            -- Valores financeiros (formato GFAR: "0000000034679700,00")
            {{ parse_financial_value('vr_empreendimento_far') }} as valor_far,
            {{ parse_financial_value('vr_total_contrapartidas') }} as valor_contrapartidas,

            -- Infraestrutura do entorno (indicadores binários)
            case when trim(ic_energia_eletrica_iluminacao_publica) = 'S' then true else false end as ic_energia_eletrica,
            case when trim(ic_agua_potavel) = 'S' then true else false end as ic_agua_potavel,
            case when trim(ic_rede_esgoto_coleta_lixo) = 'S' then true else false end as ic_esgoto,
            case when trim(ic_via_pavimentada) = 'S' then true else false end as ic_via_pavimentada,
            case when trim(ic_drenagem_pluvial) = 'S' then true else false end as ic_drenagem,
            case when trim(ic_educacao_infantil) = 'S' then true else false end as ic_educacao_infantil,
            case when trim(ic_unidade_saude_ubs) = 'S' then true else false end as ic_saude_ubs,
            case when trim(ic_terreno_doado) = 'S' then true else false end as ic_terreno_doado,

            -- Datas
            {{ target.schema }}.parse_date_br(dt_protocolo) as dt_protocolo,
            {{ target.schema }}.parse_date_br(dt_recebimento_gfar) as dt_recebimento_gfar,
            {{ target.schema }}.parse_date_br(dt_movimento) as dt_movimento,
            {{ target.schema }}.parse_date_br(dt_vencimento_portaria_contratacao) as dt_vencimento_portaria,

            -- Metadados
            arquivo_de_origem,
            criado_em

        from {{ source("raw", "novo_mcmv_far_consolidado") }}
    )

select *
from consolidado_raw
