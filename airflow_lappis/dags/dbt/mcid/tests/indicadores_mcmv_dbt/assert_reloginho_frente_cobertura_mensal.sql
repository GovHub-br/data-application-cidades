-- Teste singular: cada (agente_financeiro, frente_mcmv) do reloginho deve ter
-- série mensal CONTÍNUA entre o primeiro e o último mês observados — sem buracos
-- no meio. Retorna uma linha por combinação com lacuna, indicando quantos meses
-- faltam. Não falha por a série começar depois ou terminar antes de outra frente
-- (isso é cobertura relativa, documentada em
-- docs/entregas/issue-130-refatoracao-medalhao-reloginho.md); falha só por
-- descontinuidade interna.

with

serie as (
    select distinct
        agente_financeiro,
        frente_mcmv,
        dt_referencia
    from {{ ref("indicadores_reloginho_frente") }}
),

janela as (
    select
        agente_financeiro,
        frente_mcmv,
        min(dt_referencia) as dt_inicio,
        max(dt_referencia) as dt_fim,
        count(*) as meses_observados,
        date_diff('month', min(dt_referencia), max(dt_referencia)) + 1 as meses_esperados
    from serie
    group by agente_financeiro, frente_mcmv
)

select
    agente_financeiro,
    frente_mcmv,
    dt_inicio,
    dt_fim,
    meses_observados,
    meses_esperados,
    meses_esperados - meses_observados as meses_faltantes
from janela
where meses_observados < meses_esperados
