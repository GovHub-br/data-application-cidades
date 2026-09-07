{{ config(materialized="table", schema="mcmv_historico") }}

-- SILVER — evolução mensal de obra por empreendimento MCMV (curva prevista ×
-- realizada, situação de obra, ciclo de paralisação/retomada, decomposição de
-- UH por estado, datas de marco).
--
-- Une as 3 bronzes da família obra_mensal (FAR / FDS / Rural) — schemas
-- DIVERGENTES, resolvidos por projeção explícita com coalesce_present por lista
-- de aliases (macro silver_obra_mensal_arm). Grão de saída:
-- (frente_mcmv, apf, dt_referencia). Dedup por (apf, dt_referencia) mantendo o
-- snapshot de dt_movimento mais recente (a bronze já traz 1 arquivo/mês e
-- 1 linha/APF — a dedup é defensiva).
--
-- Janela REAL ≈ 2025-12 → 2026-07 (FAR sem 2026-06). NÃO há histórico de obra
-- mensal antes de 2025-12 — os semanais de 2025-04+ ficaram de fora (Open
-- Question 1). `pc_obra_prevista` e `co_situacao_obra` são ~100% preenchidos;
-- `dt_paralisacao` é 0% na fonte hoje (ver schema.yml).
--
-- D4: esta silver NÃO é incorporada por left join ao contrato comum das silvers
-- históricas por frente nesta change — é um modelo paralelo consultável por
-- (frente_mcmv, apf, dt_referencia). A costura é follow-up (Open Question 2).
--
-- Requer target staging_duckdb (as bronzes precisam existir no compile —
-- coalesce_present introspecciona a relação). Change:
-- enriquecer-quantidades-uh-e-sinais-obra-historico.
{% set familias = familias_obra_mensal() %}

with

    {% for f in familias %}
    obra_{{ f.frente | lower | replace(' ', '_') }} as (
{{ silver_obra_mensal_arm(ref(f.modelo), f.frente) }}
    ){{ "," if not loop.last }}
    {% endfor %}

    , unioned as (
        {% for f in familias %}
        select * from obra_{{ f.frente | lower | replace(' ', '_') }}
        {{ "union all" if not loop.last }}
        {% endfor %}
    ),

    dedup as (
        select
            *,
            row_number() over (
                partition by apf, dt_referencia
                order by dt_movimento desc nulls last, source_file desc
            ) as rn
        from unioned
    )

select
    md5(
        concat_ws('|', 'obra', frente_mcmv, coalesce(apf, ''), dt_referencia::text)
    ) as id_obra_snapshot,
    frente_mcmv,
    apf,
    dt_referencia,
    dt_movimento,
    pc_obra_prevista,
    pc_obra_realizada,
    co_situacao_obra,
    co_andamento_operacao,
    dt_alteracao_situacao,
    dt_paralisacao,
    co_classificacao_paralisado,
    detalhe_paralisacao,
    dt_previsao_conclusao_obra_retomada,
    dt_conclusao_obra_retomada,
    qt_uh_concluidas,
    qt_uh_alienadas,
    qt_uh_sem_habitese,
    qt_uh_construcao_parcial,
    qt_uh_ociosas_retomadas,
    qt_unidades_habitacionais_invadidas,
    dt_conclusao_obra,
    dt_legalizacao,
    dt_previsao_entrega,
    dt_entrega,
    dt_acion_seguradora,
    dt_contrata_construtor_substituto,
    dt_repactuacao,
    source_file,
    hash_linha,
    dt_ingest,
    current_timestamp as dt_silver
from dedup
where rn = 1
