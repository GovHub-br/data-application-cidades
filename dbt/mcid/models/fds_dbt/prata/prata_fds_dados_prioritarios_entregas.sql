{{ config(materialized="table") }}

-- Prata: Dados Prioritários CAIXA — Entregas por empreendimento
-- Fonte: bronze.bronze_sftp_snh_pmcmv_dados_prioritarios_af_caixa_entregas (a prata
-- filtra FDS)
-- Esta tabela contém TODAS as linhas (FAR, FDS, etc). O filtro por programa é
-- feito por quem consome, no JOIN com o cadastro PJ do FDS.
-- Campos-chave: qt_uh_entregues, dt_entrega (série temporal de entregas)
with
    entregas_raw as (
        select
            -- Identificação (APF da CAIXA, formato pode variar)
            {{ var('schema_udfs') }}.normalize_apf(apf) as apf,

            -- Agente financeiro
            nullif(trim(agente_financeiro), '') as agente_financeiro,

            -- Entregas
            {{ parse_int("qt_uh_entregues") }} as qt_uh_entregues,

            -- Datas
            {{ var('schema_udfs') }}.parse_date_br(dt_entrega) as dt_entrega,
            {{ var('schema_udfs') }}.parse_date_br(data_de_movimento) as dt_movimento,

            -- Metadados
            _source_file as arquivo_de_origem,
            nullif(trim(_ingested_at), '')::timestamp as criado_em

        from {{ ref("bronze_sftp_snh_pmcmv_dados_prioritarios_af_caixa_entregas") }}
    )

select *
from entregas_raw
