{{ config(materialized="table") }}

-- Bronze: Obra Mensal Rural — evolução física do empreendimento
-- Fonte: mcmv_staging.novo_mcmv_rural_obra_mensal (staging/sharepoint)
-- Cópia fiel. APF de nu_apf.

with
    obra_raw as (
        select
            {{ normalize_apf('nu_apf') }} as apf,

            {{ parse_int('co_situacao_operacao') }} as co_situacao_operacao,
            {{ parse_date_br('dt_alteracao_situacao') }} as dt_alteracao_situacao,
            {{ parse_int('co_andamento_operacao') }} as co_andamento_operacao,
            {{ parse_date_br('dt_alteracao_andamento') }} as dt_alteracao_andamento,

            {{ parse_numeric('pc_obra_prevista', 'numeric(6, 2)') }} as pct_obra_prevista,
            {{ parse_numeric('pc_obra_realizada', 'numeric(6, 2)') }} as pct_obra_realizada,

            {{ parse_int('qt_uh_concluidas') }} as qt_uh_concluidas,
            {{ parse_int('qt_uh_concluidas_adaptadas') }} as qt_uh_concluidas_adaptadas,
            {{ parse_int('qt_uh_alienada') }} as qt_uh_alienadas,
            {{ parse_int('nu_qt_uh_a_alienar') }} as qt_uh_a_alienar,
            {{ parse_int('qt_uh_sem_habitese') }} as qt_uh_sem_habitese,
            {{ parse_int('qt_uh_em_construcao_parcial') }} as qt_uh_construcao_parcial,
            {{ parse_int('qt_uh_ociosas_retomadas') }} as qt_uh_ociosas_retomadas,
            {{ parse_int('qt_uh_registro_titulos') }} as qt_uh_registro_titulos,
            {{ parse_int('qt_uh_ocupacao_irregular') }} as qt_uh_ocupacao_irregular,

            {{ parse_hist_numeric('vr_total_uh_alienadas') }} as vr_uh_alienadas,
            {{ parse_hist_numeric('vr_total_uh_a_alienar') }} as vr_uh_a_alienar,
            {{ parse_hist_numeric('vr_total_uh_sem_habitese') }} as vr_uh_sem_habitese,
            {{ parse_hist_numeric('vr_total_uh_em_construcao') }} as vr_uh_em_construcao,
            {{ parse_hist_numeric('vr_total_uh_ociosas_retomadas') }} as vr_uh_ociosas_retomadas,

            case when trim(ic_invadido) = 'S' then true else false end as ic_invadido,
            {{ parse_date_br('dt_invasao') }} as dt_invasao,
            nullif(nullif(trim(no_providencias_adotadas), ''), 'None') as providencias_invasao,

            {{ parse_date_br('dt_paralisacao') }} as dt_paralisacao,
            {{ parse_int('co_classificacao_paralisados') }} as co_classificacao_paralisado,
            {{ parse_int('co_classificacao_nao_retomada') }} as co_classificacao_nao_retomada,
            {{ parse_int('co_motivo_desimobilizacao') }} as co_motivo_desimobilizacao,
            {{ parse_int('co_motivo_distrato_empreendimento') }} as co_motivo_distrato,

            {{ parse_date_br('dt_previsao_conclusao_obra_retomada') }} as dt_previsao_conclusao_retomada,
            {{ parse_date_br('dt_conclusao_obra_retomada') }} as dt_conclusao_retomada,
            {{ parse_date_br('dt_conclusao_obra') }} as dt_conclusao_obra,
            {{ parse_date_br('dt_legalizacao_reg') }} as dt_legalizacao,
            {{ parse_date_br('dt_previsao_entrega_do_empreendimento') }} as dt_previsao_entrega,
            {{ parse_date_br('dt_entrega_do_empreendimento') }} as dt_entrega,
            {{ parse_date_br('dt_primeira_assinatura_pf') }} as dt_primeira_assinatura_pf,
            {{ parse_date_br('dt_ultima_assinatura_pf') }} as dt_ultima_assinatura_pf,
            {{ parse_date_br('dh_movimento') }} as dt_movimento,

            coalesce(nullif(trim(arquivo_de_origem), ''), _source_file) as source_file,
            coalesce(try_cast(_ingested_at as timestamp), current_timestamp) as dt_ingest,
            _source_hash as hash_linha,
            {{ hist_dt_referencia_from_filename('arquivo_de_origem') }} as dt_referencia

        from {{ source("mcmv_staging", "novo_mcmv_rural_obra_mensal") }}
    )

select *
from obra_raw
