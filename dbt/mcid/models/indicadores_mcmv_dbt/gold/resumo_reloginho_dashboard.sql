{{ config(materialized="table") }}

-- Gold do reloginho para dashboard: uma linha por agente_financeiro com o
-- último mês observado da série mensal SNH e o ritmo médio mensal de entregas.
--
-- ritmo_medio_mensal = uh_entregues(último mês) / n_meses_observados
-- (#130: entregas acumuladas / meses observados). O uh_entregues é o acumulado
-- mensal de historico_recente_* (não o fluxo o_recente_*, que é evento).
--
-- Target obrigatório: staging_duckdb (gating em dbt_project.yml).
with

    base as (select * from {{ ref("indicadores_reloginho") }}),

    ultimo_mes as (
        select agente_financeiro, max(dt_referencia) as dt_ultimo_mes
        from base
        group by agente_financeiro
    )

select
    b.agente_financeiro,
    b.dt_referencia as dt_ultimo_mes,
    b.uh_contratadas as uh_contratadas_ultimo,
    b.uh_entregues as uh_entregues_ultimo,
    b.uh_vigentes as uh_vigentes_ultimo,
    b.n_apf as n_apf_ultimo,
    b.n_meses_observados,
    round(
        b.uh_entregues::double / nullif(b.n_meses_observados, 0), 2
    ) as ritmo_medio_mensal
from base b
inner join
    ultimo_mes u
    on b.agente_financeiro = u.agente_financeiro
    and b.dt_referencia = u.dt_ultimo_mes
order by b.agente_financeiro
