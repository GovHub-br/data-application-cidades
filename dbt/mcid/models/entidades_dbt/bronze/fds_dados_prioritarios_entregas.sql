{{ config(materialized="table") }}

-- Bronze: Dados Prioritários CAIXA — Entregas por empreendimento
-- Fonte: dados_prioritarios_recebidos_caixa_entregas (11.540 registros, 7 colunas)
-- Esta tabela contém TODAS as linhas (FAR, FDS, etc). O filtro por programa
-- será feito na silver via JOIN com cadastro_pj FDS.
-- Campos-chave: qt_uh_entregues, dt_entrega (série temporal de entregas)

with
    entregas_raw as (
        select
            -- Identificação (APF da CAIXA, formato pode variar)
            {{ target.schema }}.normalize_apf(apf) as apf,

            -- Agente financeiro
            nullif(trim(agente_financeiro), '') as agente_financeiro,

            -- Entregas
            {{ parse_int('qt_uh_entregues') }} as qt_uh_entregues,

            -- Datas
            {{ target.schema }}.parse_date_br(dt_entrega) as dt_entrega,
            {{ target.schema }}.parse_date_br(data_de_movimento) as dt_movimento,

            -- Metadados
            arquivo_de_origem,
            criado_em

        from {{ source("raw", "dados_prioritarios_recebidos_caixa_entregas") }}
    )

select *
from entregas_raw
