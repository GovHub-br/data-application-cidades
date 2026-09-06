{{ config(materialized="table", schema="mcmv_historico") }}

-- SILVER — espinha de entregas por APF (change enriquecer-datas-acompanhamento-historico, B).
--
-- Grão: 1 linha por `apf`. Consolida os EVENTOS de entrega de UH da SNH
-- (bronze_reloginho_snh_entregas_evento_bb / _caixa — a mesma fonte que o
-- reloginho já ingere; o reloginho continua consumindo em paralelo) num
-- atributo estável do empreendimento: quando entregou pela primeira/última vez,
-- quantas UH ao todo, quantos eventos.
--
-- Por que existe: o feed SFTP/GEFUS de FAR e Rural congelou em 2024-11 e o FDS
-- nunca projetou data de entrega. As silvers históricas por frente fazem
-- `left join` nesta espinha por `apf` e resolvem
-- `dt_entrega_uh = coalesce(<braço SFTP>, espinha.dt_ultima_entrega)` — refresca
-- FAR/Rural além de 2024-11 e dá ao FDS uma data real.
--
-- Dedup: o mesmo evento reaparece em snapshots mensais seguintes. Deduplica por
-- hash de conteúdo de negócio (agente, apf, dt_evento, qtd) — IDÊNTICO ao de
-- silver_historico_snh_entregas_mes do reloginho, para os totais reconciliarem
-- (ver tests/mcmv_historico/assert_entrega_apf_reconcilia_reloginho.sql).
--
-- Datas: `dt_evento` já vem como DATE do bronze (parse em corpos_bronze.sql).
-- Eventos com `dt_evento` nulo ficam de fora (hoje são 0 nos dois lotes) — o
-- mesmo recorte de silver_historico_snh_entregas_mes, para reconciliar.
--
-- Destino conforme o target (D2): arquivo local em `staging_duckdb`, Postgres
-- atachado em `prod_duckdb`.
{% set entregas_familias = familias_snh_entregas() %}

with

    eventos as (
        {% for f in entregas_familias %}
        select
            coalesce(
                upper(nullif(trim(cast(agente_financeiro as varchar)), '')),
                agente_arquivo
            ) as agente_financeiro,
            nullif(trim(cast(apf as varchar)), '') as apf,
            dt_evento,
            coalesce(qt_uh_entregues_evento, 0) as qt_uh_entregues_evento,
            dt_referencia as dt_snapshot
        from {{ ref(f.modelo) }}
        where nullif(trim(cast(apf as varchar)), '') is not null
        {{ "union all" if not loop.last }}
        {% endfor %}
    ),

    hashed as (
        select
            *,
            md5(
                concat_ws(
                    '|',
                    coalesce(agente_financeiro, '␀NULL␀'),
                    coalesce(apf, '␀NULL␀'),
                    coalesce(cast(dt_evento as varchar), '␀NULL␀'),
                    coalesce(cast(qt_uh_entregues_evento as varchar), '␀NULL␀')
                )
            ) as conteudo_hash
        from eventos
    ),

    dedup as (
        select
            *,
            row_number() over (
                partition by conteudo_hash order by dt_snapshot
            ) as rn
        from hashed
    )

select
    apf,
    -- um APF pertence a um agente; se aparecer nos dois lotes, fica o do evento mais recente
    (array_agg(agente_financeiro order by dt_snapshot desc))[1] as agente_financeiro,
    min(dt_evento) as dt_primeira_entrega,
    max(dt_evento) as dt_ultima_entrega,
    sum(qt_uh_entregues_evento)::bigint as uh_entregues_acumulada,
    count(*)::bigint as n_eventos,
    max(dt_snapshot) as dt_ultimo_snapshot
from dedup
where rn = 1 and dt_evento is not null and agente_financeiro is not null
group by apf
