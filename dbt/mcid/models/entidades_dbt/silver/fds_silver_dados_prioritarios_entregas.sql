{{ config(materialized="table", alias="silver_dados_prioritarios_entregas") }}

-- Silver: Dados Prioritários CAIXA — Entregas por empreendimento
-- Fonte: empreendimentos_fds.bronze_dados_prioritarios_entregas
-- Esta tabela contém TODAS as linhas (FAR, FDS, etc). O filtro por programa é
-- feito por quem consome, no JOIN com o cadastro PJ do FDS.
-- Campos-chave: qt_uh_entregues, dt_entrega (série temporal de entregas)
with
    entregas_raw as (
        select
            -- Identificação (APF da CAIXA, formato pode variar)
            {{ target.schema }}.normalize_apf(apf) as apf,

            -- Agente financeiro
            nullif(trim(agente_financeiro), '') as agente_financeiro,

            -- Entregas
            {{ parse_int("qt_uh_entregues") }} as qt_uh_entregues,

            -- Datas
            {{ target.schema }}.parse_date_br(dt_entrega) as dt_entrega,
            {{ target.schema }}.parse_date_br(data_de_movimento) as dt_movimento,

            -- Metadados
            _source_file as arquivo_de_origem,
            nullif(trim(_ingested_at), '')::timestamp as criado_em

        from {{ source("bronze_fds", "bronze_dados_prioritarios_entregas") }}
    )

select *
from entregas_raw
