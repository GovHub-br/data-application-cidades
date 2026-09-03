{{ config(materialized="table") }}

-- Bronze: Consolidado GFAR — propostas e dados de seleção/contratação (frente FAR)
-- Fonte: mcmv_staging.novo_mcmv_far_consolidado (staging/sharepoint via MinIO/DuckDB)
-- Cópia fiel: só tipagem/normalização técnica, sem dedup. Grão: 1 linha por linha da fonte.
-- Target obrigatório: staging_duckdb (gating em dbt_project.yml).

with
    consolidado_raw as (
        select
            -- Identificação da proposta
            trim(no_identificacao_proposta) as id_proposta,
            {{ normalize_apf('nu_apf') }} as apf,
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
            {{ parse_hist_numeric('vr_empreendimento_far') }} as valor_far,
            {{ parse_hist_numeric('vr_total_contrapartidas') }} as valor_contrapartidas,

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
            {{ parse_date_br('dt_protocolo') }} as dt_protocolo,
            {{ parse_date_br('dt_recebimento_gfar') }} as dt_recebimento_gfar,
            {{ parse_date_br('dt_movimento') }} as dt_movimento,
            {{ parse_date_br('dt_vencimento_portaria_contratacao') }} as dt_vencimento_portaria,

            -- Colunas técnicas (padrão medalhão §6)
            coalesce(nullif(trim(arquivo_de_origem), ''), _source_file) as source_file,
            coalesce(try_cast(_ingested_at as timestamp), current_timestamp) as dt_ingest,
            _source_hash as hash_linha,
            {{ hist_dt_referencia_from_filename('arquivo_de_origem') }} as dt_referencia

        from {{ source("mcmv_staging", "novo_mcmv_far_consolidado") }}
    )

select *
from consolidado_raw
