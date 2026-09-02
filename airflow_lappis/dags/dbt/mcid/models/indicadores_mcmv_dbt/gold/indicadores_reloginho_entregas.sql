{{ config(materialized="table") }}

-- GOLD do reloginho (grupo A) — entregas: fluxo por evento vs acumulado do
-- snapshot, lado a lado, para a decisao #5 da #130 (qual e o total oficial de
-- UH entregues) e para alimentar o ritmo_recente.
--
-- Grao: (agente_financeiro, dt_referencia).
--   uh_entregues_evento_mes   = UH entregues NO mes (fluxo, silver de evento)
--   uh_entregues_evento_acum  = soma corrida do fluxo ate o mes
--   uh_entregues_snapshot     = acumulado reportado no snapshot (indicadores_reloginho)
--   dif_evento_vs_snapshot    = evento_acum - snapshot (deve tender a ~0)
--
-- Target obrigatorio: staging_duckdb (gating em dbt_project.yml).

with

evento_mes as (
    select
        agente_financeiro,
        mes_evento as dt_referencia,
        sum(uh_entregues_evento_mes) as uh_entregues_evento_mes,
        sum(n_eventos) as n_eventos,
        count(distinct apf) as n_apf_evento
    from {{ ref("silver_reloginho_snh_entregas_mes") }}
    group by agente_financeiro, mes_evento
),

evento_acum as (
    select
        *,
        sum(uh_entregues_evento_mes) over (
            partition by agente_financeiro
            order by dt_referencia
            rows between unbounded preceding and current row
        ) as uh_entregues_evento_acum
    from evento_mes
),

snapshot as (
    select
        agente_financeiro,
        dt_referencia,
        uh_entregues as uh_entregues_snapshot
    from {{ ref("indicadores_reloginho") }}
)

select
    coalesce(e.agente_financeiro, s.agente_financeiro) as agente_financeiro,
    coalesce(e.dt_referencia, s.dt_referencia) as dt_referencia,
    e.uh_entregues_evento_mes,
    e.uh_entregues_evento_acum,
    e.n_eventos,
    e.n_apf_evento,
    s.uh_entregues_snapshot,
    e.uh_entregues_evento_acum - s.uh_entregues_snapshot as dif_evento_vs_snapshot
from evento_acum e
full outer join snapshot s
    on e.agente_financeiro = s.agente_financeiro
   and e.dt_referencia = s.dt_referencia
order by agente_financeiro, dt_referencia
