{{ config(materialized="table") }}

-- GOLD do reloginho (grupo A) — entregas: fluxo por evento vs acumulado do
-- snapshot, lado a lado, para a decisao #5 da #130 (qual e o total oficial de
-- UH entregues) e para alimentar o ritmo_recente.
--
-- Grao: (agente_financeiro, dt_referencia).
-- uh_entregues_evento_mes   = UH entregues NO mes (fluxo, silver de evento)
-- uh_entregues_evento_acum  = soma corrida do fluxo ate o mes
-- uh_entregues_snapshot     = acumulado reportado no snapshot (indicadores_reloginho)
-- dif_evento_vs_snapshot    = evento_acum - snapshot (deve tender a ~0)
--
-- Destino conforme o target: `staging_duckdb` materializa no arquivo DuckDB
-- local (modo A, dev), `prod_duckdb` no Postgres atachado (modo C); a
-- publicação a partir do arquivo local é o modo B (./publicar-historico.sh).
-- O corpo é o mesmo nos três — ver models/mcmv_historico_dbt/README.md.
with

    evento_mes as (
        select
            agente_financeiro,
            mes_evento as dt_referencia,
            sum(uh_entregues_evento_mes) as uh_entregues_evento_mes,
            sum(n_eventos) as n_eventos,
            count(distinct apf) as n_apf_evento
        from {{ ref("prata_historico_snh_entregas_mes") }}
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
        select agente_financeiro, dt_referencia, uh_entregues as uh_entregues_snapshot
        from {{ ref("ouro_reloginho_indicadores") }}
    )

-- cast p/ bigint em todas as colunas de contagem: sum(bigint) -> HUGEINT
-- (int128) no DuckDB, sem tipo equivalente no Postgres (modo C).
-- Change: verificar-tipagem-silver-gold-historico.
select
    coalesce(e.agente_financeiro, s.agente_financeiro) as agente_financeiro,
    coalesce(e.dt_referencia, s.dt_referencia) as dt_referencia,
    cast(e.uh_entregues_evento_mes as bigint) as uh_entregues_evento_mes,
    cast(e.uh_entregues_evento_acum as bigint) as uh_entregues_evento_acum,
    cast(e.n_eventos as bigint) as n_eventos,
    e.n_apf_evento,
    cast(s.uh_entregues_snapshot as bigint) as uh_entregues_snapshot,
    cast(
        e.uh_entregues_evento_acum - s.uh_entregues_snapshot as bigint
    ) as dif_evento_vs_snapshot
from evento_acum e
full outer join
    snapshot s
    on e.agente_financeiro = s.agente_financeiro
    and e.dt_referencia = s.dt_referencia
order by agente_financeiro, dt_referencia
