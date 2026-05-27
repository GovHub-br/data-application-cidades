{{ config(materialized="table") }}

-- Bronze: Cadastro PJ Mensal — dados do empreendimento FDS (Entidades)
-- Fonte: novo_mcmv_fds_cad_pj_mensal
-- Saída: dados cadastrais limpos e tipados, incluindo Entidade Organizadora

with
    cad_pj_raw as (
        select
            -- Identificação
            {{ target.schema }}.normalize_apf(nu_apf) as apf,
            nullif(trim(no_identificacao_proposta), '') as id_proposta,
            nullif(trim(no_agente_financeiro), '') as agente_financeiro,
            nullif(trim(no_empreendimento), '') as empreendimento_nome,

            -- Entidade Organizadora (EO)
            nullif(trim(no_eo), '') as eo_nome,
            nullif(regexp_replace(trim(nu_cnpj_eo), '[^0-9]', '', 'g'), '') as eo_cnpj,
            {{ parse_int('co_nivel_hab_eo') }} as co_nivel_hab_eo,

            -- Substituição de EO
            case when trim(ic_substituicao_eo) = 'S' then true else false end as ic_substituicao_eo,
            {{ target.schema }}.parse_date_br(dt_substituicao_eo) as dt_substituicao_eo,
            nullif(trim(no_substituicao_eo), '') as eo_substituta_nome,
            nullif(regexp_replace(trim(nu_cnpj_substituicao_eo), '[^0-9]', '', 'g'), '') as eo_substituta_cnpj,

            -- Construtora
            nullif(trim(no_construtora), '') as construtora_nome,
            nullif(regexp_replace(trim(nu_cnpj_construtora), '[^0-9]', '', 'g'), '') as construtora_cnpj,

            -- Localização
            nullif(trim(no_municipio), '') as municipio,
            nullif(trim(sg_uf), '') as uf,
            nullif(trim(co_municipio_ibge), '') as cod_ibge,
            nullif(trim(no_logradouro), '') as logradouro,
            nullif(trim(no_bairro), '') as bairro,
            nullif(trim(co_cep), '') as cep,

            -- Tipologia
            {{ parse_int('co_tipo_edificacao') }} as co_tipo_edificacao,
            {{ parse_int('co_regime_obra') }} as co_regime_obra,
            {{ parse_int('co_modalidade') }} as co_modalidade,
            {{ parse_int('co_tipo_empreendimento') }} as co_tipo_empreendimento,
            {{ parse_int('co_modalidade_projeto') }} as co_modalidade_projeto,
            {{ parse_int('co_modalidade_obra') }} as co_modalidade_obra,

            -- UHs
            {{ parse_int('nu_qt_uh_construcao') }} as qt_uh_construcao,
            {{ parse_int('nu_qt_uh_projeto') }} as qt_uh_projeto,

            -- Coordenadas GPS (formato DMS)
            {{ parse_numeric('nu_gps_latitude_grau') }} as gps_lat_grau,
            {{ parse_numeric('nu_gps_latitude_minuto') }} as gps_lat_minuto,
            {{ parse_numeric('nu_gps_latitude_segundo') }} as gps_lat_segundo,
            {{ parse_numeric('nu_gps_longitude_grau') }} as gps_long_grau,
            {{ parse_numeric('nu_gps_longitude_minuto') }} as gps_long_minuto,
            {{ parse_numeric('nu_gps_longitude_segundo') }} as gps_long_segundo,
            nullif(trim(nu_gps_datum), '') as gps_datum,
            {{ parse_numeric('nu_area_terreno') }} as area_terreno,

            -- Composição financeira do investimento
            {{ parse_financial_value('vr_financiamento_fds') }} as vr_financiamento_fds,
            {{ parse_financial_value('vr_total_contrapartidas') }} as vr_total_contrapartidas,
            {{ parse_financial_value('vr_total_investimento') }} as vr_total_investimento,
            {{ parse_financial_value('vr_total_operacao') }} as vr_total_operacao,
            {{ parse_financial_value('vr_terreno') }} as vr_terreno,
            {{ parse_financial_value('vr_ts') }} as vr_trabalho_social,
            {{ parse_financial_value('vr_equipamentos_publicos') }} as vr_equipamentos_publicos,
            {{ parse_financial_value('vr_equipamento_uso_comum') }} as vr_equipamento_uso_comum,
            {{ parse_financial_value('vr_infraestrutura_incidente') }} as vr_infraestrutura,
            {{ parse_financial_value('vr_legalizacao') }} as vr_legalizacao,
            {{ parse_financial_value('vr_regularizacao') }} as vr_regularizacao,
            {{ parse_financial_value('vr_seguros_obrigatorios') }} as vr_seguros,
            {{ parse_financial_value('vr_vigilancia_obra') }} as vr_vigilancia,

            -- Contrapartidas detalhadas
            {{ parse_financial_value('vr_contrapartida_estado_infra') }} as vr_contrapartida_estado_infra,
            {{ parse_financial_value('vr_contrapartida_estado_terreno') }} as vr_contrapartida_estado_terreno,
            {{ parse_financial_value('vr_contrapartida_estado_complementar') }} as vr_contrapartida_estado_complementar,
            {{ parse_financial_value('vr_contrapartida_municipio_infra') }} as vr_contrapartida_municipio_infra,
            {{ parse_financial_value('vr_contrapartida_municipio_terreno') }} as vr_contrapartida_municipio_terreno,
            {{ parse_financial_value('vr_contrapartida_municipio_complementar') }} as vr_contrapartida_municipio_complementar,
            {{ parse_financial_value('vr_contrapartida_outras') }} as vr_contrapartida_outras,

            -- Aporte / Retomada
            nullif(trim(nu_apf_aporte_suplementacao), '') as apf_aporte,
            {{ parse_int('co_operacao_retomada') }} as co_operacao_retomada,
            {{ parse_financial_value('vr_contratado_aporte_suplementacao') }} as vr_aporte_suplementacao,
            {{ target.schema }}.parse_date_br(dt_contratacao_aporte_suplementacao) as dt_aporte_suplementacao,
            {{ parse_financial_value('vr_total_em_construcao') }} as vr_total_em_construcao,
            {{ parse_financial_value('vr_total_em_projeto') }} as vr_total_em_projeto,

            -- Datas-chave
            {{ target.schema }}.parse_date_br(dt_assinatura) as dt_contratacao,
            {{ target.schema }}.parse_date_br(dt_inicio_obra) as dt_inicio_obra,
            {{ target.schema }}.parse_date_br(dt_previsao_conclusao_obra) as dt_previsao_conclusao,
            {{ target.schema }}.parse_date_br(dt_apresentacao_orcamento) as dt_apresentacao_orcamento,
            {{ target.schema }}.parse_date_br(dt_inicio_obra_retomada) as dt_inicio_obra_retomada,
            {{ target.schema }}.parse_date_br(dt_previsao_conclusao_obra_retomada) as dt_previsao_conclusao_retomada,
            {{ target.schema }}.parse_date_br(dh_movimento) as dt_movimento,

            -- Indicadores
            case when trim(ic_terreno_doado) = 'S' then true else false end as ic_terreno_doado,
            case when trim(ic_uniao_imovel) = 'S' then true else false end as ic_uniao_imovel,
            case when trim(ic_sistema_aquec_solar) = 'S' then true else false end as ic_aquecimento_solar,
            case when trim(ic_energia_alternativa) = 'S' then true else false end as ic_energia_alternativa,
            case when trim(ic_previsao_area_comercial) = 'S' then true else false end as ic_area_comercial,
            case when trim(ic_mudanca_fase) = 'S' then true else false end as ic_mudanca_fase,
            nullif(trim(nu_apf_mudanca_fase), '') as apf_mudanca_fase,

            -- Energia alternativa e solar (valores)
            {{ parse_financial_value('vr_sistema_alternativo_de_energia') }} as vr_energia_alternativa,
            {{ parse_financial_value('vr_sistema_aquecimento_solar') }} as vr_aquecimento_solar,

            -- Metadados
            arquivo_de_origem,
            criado_em

        from {{ source("raw", "novo_mcmv_fds_cad_pj_mensal") }}
    )

select *
from cad_pj_raw
