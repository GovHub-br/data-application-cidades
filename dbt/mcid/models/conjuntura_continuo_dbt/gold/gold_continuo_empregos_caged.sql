{{ config(materialized='table') }}

-- Gold do conjuntura contínuo: Empregos — saldo e estoque na construção
-- civil (edifícios, serviços especializados e total) x total da economia.
-- Página 3, seção 4. Fonte: Novo CAGED (PowerBI, automatizado) — substitui
-- o dado MANUAL (boletim.xlsx) usado até 2026-08-24. `var_12_meses` é
-- calculado aqui via window function sobre o estoque, pois a API do Novo
-- CAGED só publica a variação do próprio período ("variacao" = var_mes).

with edificios as (
    select
        ano, mes, saldo, estoque, variacao as var_mes,
        (estoque - lag(estoque, 12) over (order by ano, mes))
            / nullif(lag(estoque, 12) over (order by ano, mes), 0) as var_12_meses
    from {{ ref('silver_continuo_novo_caged') }}
),

servicos_especializados as (
    select
        ano, mes, saldo, estoque, variacao as var_mes,
        (estoque - lag(estoque, 12) over (order by ano, mes))
            / nullif(lag(estoque, 12) over (order by ano, mes), 0) as var_12_meses
    from {{ ref('silver_continuo_novo_caged_servicos_especializados') }}
),

total_construcao as (
    select
        ano, mes, saldo, estoque, variacao as var_mes,
        (estoque - lag(estoque, 12) over (order by ano, mes))
            / nullif(lag(estoque, 12) over (order by ano, mes), 0) as var_12_meses
    from {{ ref('silver_continuo_novo_caged_total') }}
)

select
    coalesce(ed.ano, se.ano, tc.ano) as ano,
    coalesce(ed.mes, se.mes, tc.mes) as mes,
    make_date(coalesce(ed.ano, se.ano, tc.ano)::int, coalesce(ed.mes, se.mes, tc.mes)::int, 1) as data_referencia,

    ed.saldo as edificios_saldo,
    ed.var_mes as edificios_saldo_var_mes,
    ed.var_12_meses as edificios_saldo_var_12_meses,
    ed.estoque as edificios_estoque,

    se.saldo as servicos_especializados_saldo,
    se.var_mes as servicos_especializados_saldo_var_mes,
    se.var_12_meses as servicos_especializados_saldo_var_12_meses,
    se.estoque as servicos_especializados_estoque,

    tc.saldo as total_construcao_saldo,
    tc.var_mes as total_construcao_saldo_var_mes,
    tc.var_12_meses as total_construcao_saldo_var_12_meses,
    tc.estoque as total_construcao_estoque

from edificios ed
full outer join servicos_especializados se on ed.ano = se.ano and ed.mes = se.mes
full outer join total_construcao tc on coalesce(ed.ano, se.ano) = tc.ano and coalesce(ed.mes, se.mes) = tc.mes
order by ano desc, mes desc
