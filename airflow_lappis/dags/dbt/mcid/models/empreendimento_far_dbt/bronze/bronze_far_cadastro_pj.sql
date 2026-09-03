{{ config(materialized="table") }}

-- Bronze: Cadastro PJ Mensal — dados detalhados do empreendimento contratado (frente FAR)
-- Fonte: mcmv_staging.novo_mcmv_far_cad_pj_mensal (staging/sharepoint via MinIO/DuckDB)
-- Cópia fiel: só tipagem/normalização técnica, sem dedup.

with
    cad_pj_raw as (
        select
            -- Identificação
            {{ normalize_apf('nu_apf') }} as apf,
            nullif(trim(no_identificacao_proposta), '') as id_proposta,
            nullif(trim(no_agente_financeiro), '') as agente_financeiro,
            nullif(trim(no_empreendimento), '') as empreendimento_nome,

            -- Construtora
            nullif(trim(no_construtora), '') as construtora_nome,
            nullif(regexp_replace(trim(nu_cnpj_construtora), '[^0-9]', '', 'g'), '') as construtora_cnpj,

            -- Ente público proponente
            nullif(trim(co_ente_publico_proponente), '') as co_ente_publico,

            -- Localização
            nullif(trim(no_municipio), '') as municipio,
            nullif(trim(sg_uf), '') as uf,
            nullif(trim(co_municipio_ibge), '') as cod_ibge,

            -- Tipologia
            {{ parse_int('co_tipo_edificacao') }} as co_tipo_edificacao,
            {{ parse_int('co_tipo_administracao') }} as co_tipo_administracao,
            {{ parse_int('co_linha_atendimento') }} as co_linha_atendimento,
            {{ parse_int('co_tipo_demanda') }} as co_tipo_demanda,
            {{ parse_int('co_tipo_empreendimento') }} as co_tipo_empreendimento,

            -- UHs
            {{ parse_int('nu_qt_uh') }} as qt_uh,

            -- Coordenadas GPS (formato DMS - grau/minuto/segundo)
            {{ parse_numeric('nu_gps_latitude_grau') }} as gps_lat_grau,
            {{ parse_numeric('nu_gps_latitude_minuto') }} as gps_lat_minuto,
            {{ parse_numeric('nu_gps_latitude_segundo') }} as gps_lat_segundo,
            {{ parse_numeric('nu_gps_longitude_grau') }} as gps_long_grau,
            {{ parse_numeric('nu_gps_longitude_minuto') }} as gps_long_minuto,
            {{ parse_numeric('nu_gps_longitude_segundo') }} as gps_long_segundo,
            nullif(trim(no_gps_datum), '') as gps_datum,

            -- Composição financeira do investimento
            {{ parse_hist_numeric('vr_emprestimo_far') }} as vr_emprestimo_far,
            {{ parse_hist_numeric('vr_total_contrapartidas') }} as vr_contrapartida_ente_publico,
            {{ parse_hist_numeric('vr_total_investimento') }} as vr_total_investimento,
            {{ parse_hist_numeric('vr_obra_edificacao') }} as vr_obra_edificacao,
            {{ parse_hist_numeric('vr_terreno') }} as vr_terreno,
            {{ parse_hist_numeric('vr_ts') }} as vr_trabalho_social,
            {{ parse_hist_numeric('vr_equipamento_uso_comum') }} as vr_equipamento_uso_comum,
            {{ parse_hist_numeric('vr_legalizacao_empreendimento') }} as vr_legalizacao,
            {{ parse_hist_numeric('vr_seguros_obrigatorios') }} as vr_seguro_obrigatorio,

            -- Datas-chave
            {{ parse_date_br('dt_contratacao') }} as dt_contratacao,
            {{ parse_date_br('dt_inicio_obra') }} as dt_inicio_obra,
            {{ parse_date_br('dt_previsao_conclusao_obra') }} as dt_previsao_conclusao,
            {{ parse_date_br('dt_apresentacao_orcamento') }} as dt_apresentacao_orcamento,
            {{ parse_date_br('dt_movimento') }} as dt_movimento,

            -- Indicadores
            case when trim(ic_terreno_doado) = 'S' then true else false end as ic_terreno_doado,
            case when trim(ic_sistema_aquec_solar) = 'S' then true else false end as ic_aquecimento_solar,
            case when trim(ic_energia_alternativa) = 'S' then true else false end as ic_energia_alternativa,
            case when trim(ic_previsao_area_comercial) = 'S' then true else false end as ic_area_comercial,

            -- Colunas técnicas (padrão medalhão §6)
            coalesce(nullif(trim(arquivo_de_origem), ''), _source_file) as source_file,
            coalesce(try_cast(_ingested_at as timestamp), current_timestamp) as dt_ingest,
            _source_hash as hash_linha,
            {{ hist_dt_referencia_from_filename('arquivo_de_origem') }} as dt_referencia

        from {{ source("mcmv_staging", "novo_mcmv_far_cad_pj_mensal") }}
    )

select *
from cad_pj_raw
