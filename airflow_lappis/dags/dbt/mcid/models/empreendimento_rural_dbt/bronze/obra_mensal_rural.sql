{{ config(materialized="table") }}

-- Bronze: Evolução física mensal da obra (Rural)
-- Fonte: novo_mcmv_rural_obra_mensal
-- Saída: dados de andamento da obra limpos e tipados por APF

with
    obra_mensal_raw as (
        select
            -- Identificação
            {{ target.schema }}.normalize_apf(nu_apf) as apf,
            
            -- Situação e Andamento
            {{ parse_int('co_situacao_operacao') }} as co_situacao_operacao,
            {{ parse_int('co_andamento_operacao') }} as co_andamento_operacao,

            -- Evolução física (%)
            {{ parse_numeric('pc_obra_prevista', 'numeric(6,2)') }} as percentual_obra_prevista,
            {{ parse_numeric('pc_obra_realizada', 'numeric(6,2)') }} as percentual_obra_realizada,

            -- UHs Concluídas/Alienadas/Sem Habite-se
            {{ parse_int('qt_uh_concluidas') }} as qt_uh_concluidas,
            {{ parse_int('qt_uh_concluidas_adaptadas') }} as qt_uh_concluidas_adaptadas,
            {{ parse_int('qt_uh_alienada') }} as qt_uh_alienadas,
            {{ parse_int('nu_qt_uh_a_alienar') }} as qt_uh_a_alienar,
            {{ parse_int('qt_uh_sem_habitese') }} as qt_uh_sem_habitese,
            {{ parse_int('qt_uh_em_construcao_parcial') }} as qt_uh_em_construcao_parcial,
            {{ parse_int('qt_uh_ociosas_retomadas') }} as qt_uh_ociosas_retomadas,

            -- Valores
            {{ parse_financial_value('vr_total_uh_alienadas') }} as vr_total_uh_alienadas,
            {{ parse_financial_value('vr_total_uh_a_alienar') }} as vr_total_uh_a_alienar,
            {{ parse_financial_value('vr_total_uh_sem_habitese') }} as vr_total_uh_sem_habitese,
            {{ parse_financial_value('vr_total_uh_em_construcao') }} as vr_total_uh_em_construcao,
            {{ parse_financial_value('vr_total_uh_ociosas_retomadas') }} as vr_total_uh_ociosas_retomadas,

            -- Indicadores
            case when trim(ic_invadido) = 'S' then true else false end as ic_invadido,
            case when trim(ic_titularidade) = 'S' then true else false end as ic_titularidade,

            -- Paralisados e Motivos
            {{ parse_int('co_classificacao_paralisados') }} as co_classificacao_paralisados,
            {{ parse_int('co_classificacao_nao_retomada') }} as co_classificacao_nao_retomada,
            nullif(trim(no_detalhe_paralisacao_retomada), '') as detalhe_paralisacao_retomada,
            {{ parse_int('co_motivo_desimobilizacao') }} as co_motivo_desimobilizacao,
            {{ parse_int('co_motivo_distrato_empreendimento') }} as co_motivo_distrato,

            -- Datas
            {{ target.schema }}.parse_date_br(dh_movimento) as dt_movimento,
            {{ target.schema }}.parse_date_br(dt_alteracao_situacao) as dt_alteracao_situacao,
            {{ target.schema }}.parse_date_br(dt_alteracao_andamento) as dt_alteracao_andamento,
            {{ target.schema }}.parse_date_br(dt_invasao) as dt_invasao,
            {{ target.schema }}.parse_date_br(dt_previsao_conclusao_obra_retomada) as dt_previsao_conclusao_obra_retomada,
            {{ target.schema }}.parse_date_br(dt_conclusao_obra_retomada) as dt_conclusao_obra_retomada,
            {{ target.schema }}.parse_date_br(dt_primeira_assinatura_pf) as dt_primeira_assinatura_pf,
            {{ target.schema }}.parse_date_br(dt_ultima_assinatura_pf) as dt_ultima_assinatura_pf,
            {{ target.schema }}.parse_date_br(dt_paralisacao) as dt_paralisacao,
            {{ target.schema }}.parse_date_br(dt_conclusao_obra) as dt_conclusao_obra,
            {{ target.schema }}.parse_date_br(dt_previsao_entrega_do_empreendimento) as dt_previsao_entrega,
            {{ target.schema }}.parse_date_br(dt_entrega_do_empreendimento) as dt_entrega_empreendimento,

            -- Metadados
            arquivo_de_origem,
            criado_em

        from {{ source("raw", "novo_mcmv_rural_obra_mensal") }}
    )

select *
from obra_mensal_raw
