{{ config(materialized="table") }}

-- Bronze: Obra Mensal — evolução física do empreendimento
-- Fonte: novo_mcmv_far_obra_mensal (822 registros, 40 colunas text)
-- Saída: dados de obra limpos e tipados

with
    obra_raw as (
        select
            -- Identificação
            {{ target.schema }}.normalize_apf(nu_apf) as apf,

            -- Situação da obra
            {{ parse_int('co_situacao_obra') }} as co_situacao_obra,
            {{ target.schema }}.parse_date_br(dt_alteracao_situacao) as dt_alteracao_situacao,

            -- Percentuais de execução
            {{ parse_numeric('pc_obra_prevista', 'numeric(6, 2)') }} as pct_obra_prevista,
            {{ parse_numeric('pc_obra_realizada', 'numeric(6, 2)') }} as pct_obra_realizada,
            -- UHs
            {{ parse_int('qt_uh_concluidas_adaptadas') }} as qt_uh_concluidas_adaptadas,
            {{ parse_int('qt_uh_alienada') }} as qt_uh_alienadas,
            {{ parse_int('qt_uh_sem_habitese') }} as qt_uh_sem_habitese,
            {{ parse_int('qt_uh_em_construcao_parcial') }} as qt_uh_construcao_parcial,
            {{ parse_int('qt_uh_ociosas_retomadas') }} as qt_uh_ociosas_retomadas,
            {{ parse_int('qt_unidades_habitacionais_invadidas') }} as qt_uh_invadidas,

            -- Valores de UHs
            {{ parse_financial_value('vr_total_uh_alienadas') }} as vr_uh_alienadas,
            {{ parse_financial_value('vr_total_uh_a_alienar') }} as vr_uh_a_alienar,
            {{ parse_financial_value('vr_total_uh_sem_habitese') }} as vr_uh_sem_habitese,
            {{ parse_financial_value('vr_total_uh_em_construcao') }} as vr_uh_em_construcao,
            {{ parse_financial_value('vr_total_uh_ociosas_retomadas') }} as vr_uh_ociosas_retomadas,
            nullif(nullif(trim(nu_qt_uh_a_alienar), ''), 'None')::int as qt_uh_a_alienar,

            -- Invasão
            case when trim(ic_invadido) = 'S' then true else false end as ic_invadido,
            {{ target.schema }}.parse_date_br(dt_invasao) as dt_invasao,
            nullif(nullif(trim(no_providencias_adotadas), ''), 'None') as providencias_invasao,

            -- Paralisação
            {{ target.schema }}.parse_date_br(dt_paralisacao) as dt_paralisacao,
            {{ parse_int('co_classificacao_paralisados') }} as co_classificacao_paralisado,
            {{ parse_int('co_motivo_cancelamento_distrato') }} as co_motivo_cancelamento,
            
            -- Datas de marcos da obra
            {{ target.schema }}.parse_date_br(dt_previsao_conclusao_obra_retomada) as dt_previsao_conclusao_retomada,
            {{ target.schema }}.parse_date_br(dt_conclusao_obra_retomada) as dt_conclusao_retomada,
            {{ target.schema }}.parse_date_br(dt_conclusao_obra) as dt_conclusao_obra,
            {{ target.schema }}.parse_date_br(dt_repactuacao) as dt_repactuacao,
            {{ target.schema }}.parse_date_br(dt_legalizacao) as dt_legalizacao,

            -- Datas de entrega
            {{ target.schema }}.parse_date_br(dt_previsao_entrega_do_empreendimento) as dt_previsao_entrega,
            {{ target.schema }}.parse_date_br(dt_entrega_do_empreendimento) as dt_entrega,

            -- Datas de assinatura PF (mutuários)
            {{ target.schema }}.parse_date_br(dt_primeira_assinatura_pf) as dt_primeira_assinatura_pf,
            {{ target.schema }}.parse_date_br(dt_ultima_assinatura_pf) as dt_ultima_assinatura_pf,

            -- Datas de ações corretivas
            {{ target.schema }}.parse_date_br(dt_acion_seguradora) as dt_acionamento_seguradora,
            {{ target.schema }}.parse_date_br(dt_contratacao_vigilancia) as dt_contratacao_vigilancia,
            {{ target.schema }}.parse_date_br(dt_contrata_construtor_substituto) as dt_construtor_substituto,

            -- Referência temporal
            {{ target.schema }}.parse_date_br(dt_movimento) as dt_movimento,

            -- Metadados
            arquivo_de_origem,
            criado_em

        from {{ source("raw", "novo_mcmv_far_obra_mensal") }}
    )

select *
from obra_raw
