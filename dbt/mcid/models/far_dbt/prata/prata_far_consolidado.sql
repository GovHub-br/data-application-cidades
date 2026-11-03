{{ config(materialized="table") }}

-- Prata: Consolidado GFAR — propostas e dados de seleção/contratação
-- Fonte: bronze.bronze_shpt_his_mcidades_consolidado_serie
-- Saída: apenas colunas relevantes para a ficha do empreendimento, com tipagem correta
-- Cada fase da proposta vem da última remessa em que apareceu: a remessa é a foto da
-- semana, e proposta contratada pode sumir dela, ou voltar só com a linha da seleção,
-- sem ter sido cancelada.
with
    remessas as (
        select
            *,
            max(dt_remessa) over (
                partition by trim(no_identificacao_proposta), trim(co_fase)
            ) as dt_ultima_remessa_fase,
            max(dt_remessa) over (
                partition by trim(no_identificacao_proposta)
            ) as dt_ultima_remessa,
            min(dt_remessa) over (
                partition by trim(no_identificacao_proposta)
            ) as dt_primeira_remessa,
            max(dt_remessa) over () as dt_remessa_mais_recente
        from
            (
                select
                    *,
                    to_date(
                        substring(filename from 'CONSOLIDADO_([0-9]{8})'), 'YYYYMMDD'
                    ) as dt_remessa
                from {{ ref("bronze_shpt_his_mcidades_consolidado_serie") }}
            ) b
    ),

    consolidado_raw as (
        select
            -- Identificação da proposta
            trim(no_identificacao_proposta) as id_proposta,
            {{ var('schema_udfs') }}.normalize_apf(nu_apf) as apf,
            nullif(trim(nu_operacao_af), '') as nu_operacao_af,

            -- Portaria e situação
            nullif(trim(nu_portaria_contratacao), '') as portaria_contratacao,
            {{ parse_int("co_situacao_proposta_contratada") }} as co_situacao_contratada,
            {{ parse_int("co_situacao_proposta_protocolizada") }}
            as co_situacao_protocolizada,

            -- Fase/etapa do fluxo GFAR
            {{ parse_int("co_fase") }} as co_fase,
            {{ parse_int("co_etapa") }} as co_etapa,
            {{ parse_int("co_status") }} as co_status,

            -- Proponente
            nullif(trim(no_proponente), '') as proponente_nome,
            nullif(
                regexp_replace(trim(nu_cnpj_proponente), '[^0-9]', '', 'g'), ''
            ) as proponente_cnpj,
            {{ parse_int("co_tipo_de_proponente") }} as co_tipo_proponente,

            -- Tomador
            nullif(trim(no_tomador), '') as tomador_nome,
            nullif(
                regexp_replace(trim(nu_cnpj_tomador), '[^0-9]', '', 'g'), ''
            ) as tomador_cnpj,

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
            {{ parse_int("co_tipo_edificacao") }} as co_tipo_edificacao,
            coalesce(
                {{ parse_int("nu_qt_uh_empreendimento_contratacao") }},
                {{ parse_int("nu_qt_uh_empreendimento") }}
            ) as qt_uh,

            -- Demanda e terreno
            {{ parse_int("co_tipo_de_demanda") }} as co_tipo_demanda,
            {{ parse_int("co_tipo_demanda_emp_contratacao") }}
            as co_tipo_demanda_contratacao,
            {{ parse_int("co_originacao_empreendimento") }} as co_originacao,
            -- A da contratação quase sempre vem vazia; cai para a do enquadramento
            coalesce(
                {{ parse_int("co_qualificacao_terreno_contratacao") }},
                {{ parse_int("co_qualificacao_terreno") }}
            ) as co_qualificacao_terreno,
            case
                trim(ic_previsao_elevador) when 'S' then true when 'N' then false
            end as ic_previsao_elevador,
            trim(ic_dados_alterados_pos_contratacao)
            = 'S' as ic_alterado_pos_contratacao,
            nullif(trim(no_campo_alterado_contratacao), '')
            as campos_alterados_contratacao,

            -- Valores financeiros (formato GFAR: "0000000034679700,00")
            {{ parse_financial_value("vr_empreendimento_far") }} as valor_far,
            {{ parse_financial_value("vr_total_contrapartidas") }}
            as valor_contrapartidas,

            -- Infraestrutura do entorno (indicadores binários)
            case
                when trim(ic_energia_eletrica_iluminacao_publica) = 'S'
                then true
                else false
            end as ic_energia_eletrica,
            case
                when trim(ic_agua_potavel) = 'S' then true else false
            end as ic_agua_potavel,
            case
                when trim(ic_rede_esgoto_coleta_lixo) = 'S' then true else false
            end as ic_esgoto,
            case
                when trim(ic_via_pavimentada) = 'S' then true else false
            end as ic_via_pavimentada,
            case
                when trim(ic_drenagem_pluvial) = 'S' then true else false
            end as ic_drenagem,
            case
                when trim(ic_educacao_infantil) = 'S' then true else false
            end as ic_educacao_infantil,
            case
                when trim(ic_unidade_saude_ubs) = 'S' then true else false
            end as ic_saude_ubs,
            trim(ic_terreno_area_urbana_consolidada)
            = 'S' as ic_area_urbana_consolidada,
            trim(ic_ensino_fundamental_ciclo_1) = 'S' as ic_ensino_fundamental_1,
            trim(ic_ensino_fundamental_ciclo_2) = 'S' as ic_ensino_fundamental_2,
            trim(ic_unidade_protecao_social_cras) = 'S' as ic_protecao_social_cras,
            trim(ic_comercio_servicos_cotidiano) = 'S' as ic_comercio_servicos,
            case
                when trim(ic_terreno_doado) = 'S' then true else false
            end as ic_terreno_doado,

            -- Datas
            {{ var('schema_udfs') }}.parse_date_br(dt_protocolo) as dt_protocolo,
            {{ var('schema_udfs') }}.parse_date_br(dt_recebimento_gfar) as dt_recebimento_gfar,
            {{ var('schema_udfs') }}.parse_date_br(dt_movimento) as dt_movimento,
            nullif(trim(hr_movimento), '') as hr_movimento,
            {{ var('schema_udfs') }}.parse_date_br(
                dt_vencimento_portaria_contratacao
            ) as dt_vencimento_portaria,

            -- Remessas
            dt_remessa,
            dt_primeira_remessa,
            dt_ultima_remessa,
            dt_ultima_remessa = dt_remessa_mais_recente as ic_na_remessa_mais_recente,

            -- Metadados
            _source_file as arquivo_de_origem,
            nullif(trim(_ingested_at), '')::timestamp as criado_em

        from remessas
        where dt_remessa = dt_ultima_remessa_fase
    )

select *
from consolidado_raw
