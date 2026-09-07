{#
    Braço de uma frente na silver silver_mcmv_historico_obra_mensal (change
    enriquecer-quantidades-uh-e-sinais-obra-historico, D4).

    As 3 bronzes da família obra_mensal têm schemas DIVERGENTES (FAR:
    `dt_movimento` / `co_situacao_obra`; FDS/RURAL: `dh_movimento` /
    `co_situacao_operacao` + `co_andamento_operacao`; retomada e legalização com
    nomes distintos). Este macro projeta o contrato comum da silver a partir de
    QUALQUER uma das 3 relações, resolvendo o nome real por lista de aliases via
    coalesce_present[_parsed] — que introspecciona a bronze materializada e
    compila a coluna ausente como NULL do tipo certo (tolera drift de schema do
    parquet e a divergência entre frentes).

    Tipagem (task 5.2): pc_* -> parse_hist_double; qt_* -> parse_hist_bigint;
    dt_*/dh_* -> parse_hist_date; co_*/detalhe_* -> texto cru.
#}
{% macro silver_obra_mensal_arm(rel, frente_mcmv) %}
        select
            '{{ frente_mcmv }}'::text as frente_mcmv,
            nullif(trim(nu_apf::text), '')::text as apf,
            dt_referencia,
            {{ coalesce_present_parsed(
                rel, ['dt_movimento', 'dh_movimento'], 'parse_hist_date', 'date'
            ) }} as dt_movimento,
            -- curva de obra (sinal central desta silver)
            {{ coalesce_present_parsed(rel, ['pc_obra_prevista'], 'parse_hist_double', 'double') }}
            as pc_obra_prevista,
            {{ coalesce_present_parsed(rel, ['pc_obra_realizada'], 'parse_hist_double', 'double') }}
            as pc_obra_realizada,
            -- situação da obra/operação (código cru; domínio a decodificar em follow-up)
            nullif(trim({{ coalesce_present(rel, ['co_situacao_obra', 'co_situacao_operacao']) }}), '')::text
            as co_situacao_obra,
            nullif(trim({{ coalesce_present(rel, ['co_andamento_operacao']) }}), '')::text
            as co_andamento_operacao,
            {{ coalesce_present_parsed(rel, ['dt_alteracao_situacao'], 'parse_hist_date', 'date') }}
            as dt_alteracao_situacao,
            -- ciclo de paralisação / retomada. dt_paralisacao é 0% na fonte hoje
            -- (a coluna existe, vazia) — o sinal utilizável é co_situacao_obra / texto.
            {{ coalesce_present_parsed(rel, ['dt_paralisacao'], 'parse_hist_date', 'date') }}
            as dt_paralisacao,
            nullif(trim({{ coalesce_present(rel, ['co_classificacao_paralisados']) }}), '')::text
            as co_classificacao_paralisado,
            nullif(trim({{ coalesce_present(rel, ['detalhe_paralisacao_retomada', 'no_detalhe_paralisacao_retomada']) }}), '')::text
            as detalhe_paralisacao,
            {{ coalesce_present_parsed(
                rel, ['dt_previsao_conclusao_obra_retomada', 'dt_prev_conclusao_obra_retomada'], 'parse_hist_date', 'date'
            ) }} as dt_previsao_conclusao_obra_retomada,
            {{ coalesce_present_parsed(rel, ['dt_conclusao_obra_retomada'], 'parse_hist_date', 'date') }}
            as dt_conclusao_obra_retomada,
            -- decomposição de UH por estado
            {{ coalesce_present_parsed(rel, ['qt_uh_concluidas'], 'parse_hist_bigint', 'bigint') }}
            as qt_uh_concluidas,
            {{ coalesce_present_parsed(rel, ['qt_uh_alienada'], 'parse_hist_bigint', 'bigint') }}
            as qt_uh_alienadas,
            {{ coalesce_present_parsed(rel, ['qt_uh_sem_habitese'], 'parse_hist_bigint', 'bigint') }}
            as qt_uh_sem_habitese,
            {{ coalesce_present_parsed(rel, ['qt_uh_em_construcao_parcial'], 'parse_hist_bigint', 'bigint') }}
            as qt_uh_construcao_parcial,
            {{ coalesce_present_parsed(rel, ['qt_uh_ociosas_retomadas'], 'parse_hist_bigint', 'bigint') }}
            as qt_uh_ociosas_retomadas,
            {{ coalesce_present_parsed(
                rel, ['qt_unidades_habitacionais_invadidas', 'qt_uh_ocupacao_irregular'], 'parse_hist_bigint', 'bigint'
            ) }} as qt_unidades_habitacionais_invadidas,
            -- marcos
            {{ coalesce_present_parsed(rel, ['dt_conclusao_obra'], 'parse_hist_date', 'date') }}
            as dt_conclusao_obra,
            {{ coalesce_present_parsed(rel, ['dt_legalizacao', 'dt_legalizacao_reg'], 'parse_hist_date', 'date') }}
            as dt_legalizacao,
            {{ coalesce_present_parsed(
                rel, ['dt_previsao_entrega_do_empreendimento', 'dt_prev_entrega_emprend'], 'parse_hist_date', 'date'
            ) }} as dt_previsao_entrega,
            {{ coalesce_present_parsed(rel, ['dt_entrega_do_empreendimento'], 'parse_hist_date', 'date') }}
            as dt_entrega,
            -- só FAR
            {{ coalesce_present_parsed(rel, ['dt_acion_seguradora'], 'parse_hist_date', 'date') }}
            as dt_acion_seguradora,
            {{ coalesce_present_parsed(rel, ['dt_contrata_construtor_substituto'], 'parse_hist_date', 'date') }}
            as dt_contrata_construtor_substituto,
            {{ coalesce_present_parsed(rel, ['dt_repactuacao'], 'parse_hist_date', 'date') }}
            as dt_repactuacao,
            source_file,
            hash_linha,
            dt_ingest
        from {{ rel }}
        where nullif(trim(nu_apf::text), '') is not null
{% endmacro %}
