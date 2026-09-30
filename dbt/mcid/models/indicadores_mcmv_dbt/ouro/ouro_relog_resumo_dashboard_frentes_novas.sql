{{ config(materialized="table") }}

-- OURO do reloginho para dashboard: uma linha por frente_mcmv (Classe Média,
-- Reforma Casa Brasil, MCMV Cidades, Pró-Moradia) com o último mês observado
-- e o ritmo médio mensal de contratação (change
-- criar-ouro-reloginho-frentes-novas, design.md D2).
--
-- ritmo_medio_mensal_* = acumulado corrido do último mês / n_meses_observados
-- — mesma fórmula de `ouro_relog_resumo_dashboard`, aplicada ao
-- acumulado CALCULADO em `ouro_relog_indicadores_frentes_novas` (fonte de
-- fluxo), não a um estoque já pronto (D2). NÃO usar `n_contratos`/
-- `valor_contratado` do último mês isoladamente como ritmo — um mês pode ser
-- atípico (mês parcial, pico sazonal).
with

    base as (select * from {{ ref("ouro_relog_indicadores_frentes_novas") }}),

    ultimo_mes as (
        select frente_mcmv, max(mes_referencia) as mes_referencia_ultimo
        from base
        group by frente_mcmv
    )

select
    b.frente_mcmv,
    b.mes_referencia as mes_referencia_ultimo,
    b.n_contratos_acumulado,
    b.valor_contratado_acumulado,
    b.n_meses_observados,
    round(
        b.n_contratos_acumulado::double / nullif(b.n_meses_observados, 0), 2
    ) as ritmo_medio_mensal_contratos,
    round(
        b.valor_contratado_acumulado::double / nullif(b.n_meses_observados, 0), 2
    ) as ritmo_medio_mensal_valor
from base b
inner join
    ultimo_mes u
    on b.frente_mcmv = u.frente_mcmv
    and b.mes_referencia = u.mes_referencia_ultimo
order by b.frente_mcmv
