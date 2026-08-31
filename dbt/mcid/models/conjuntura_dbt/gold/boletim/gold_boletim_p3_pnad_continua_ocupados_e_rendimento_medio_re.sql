{{ config(materialized='table') }}

-- Boletim de Conjuntura, página 3: PNAD Contínua — Ocupados e Rendimento Médio Real
-- Seção do impresso: 4. Empregos
--
-- Uma linha por EDIÇÃO (coluna `edicao`), com as colunas na ordem
-- impressa. O filtro do Superset seleciona a edição; o dashboard não
-- calcula nada — este SQL rodava como dataset virtual e voltava ao
-- engine a cada carregamento de página.

select "edicao", "periodo", "Ocupados Construção (mil)", "Ocupados Total (mil)", "Rendimento Construção (R$)", "Rendimento Total (R$)"
from (

    with 
edicoes as (
    select distinct periodo as edicao,
           (right(periodo, 4)::int * 4 + left(periodo, 1)::int) as k,
           right(periodo, 4)::int as ano_ed,
           left(periodo, 1)::int  as tri_ed
    from {{ ref('gold_continuo_pib_construcao_civil_pct') }}
    where right(periodo, 4)::int >= 2025
),
    base as (
        select o.periodo,
               (left(o.periodo, 4)::int * 12 + right(o.periodo, 2)::int) as m,
               o.periodo_nome, o.ocupados_construcao_mil oc, o.ocupados_total_mil ot,
               r.rendimento_construcao_rs rc, r.rendimento_total_rs rt
        from {{ ref('gold_continuo_pnad_ocupados') }} o
        join {{ ref('gold_continuo_pnad_rendimento') }} r on r.periodo = o.periodo
    ),
    ref as (select edicao, ano_ed * 12 + tri_ed * 3 as m0 from edicoes)
    select r.edicao, b.periodo_nome as periodo,
           b.oc as "Ocupados Construção (mil)", b.ot as "Ocupados Total (mil)",
           b.rc as "Rendimento Construção (R$)", b.rt as "Rendimento Total (R$)",
           case b.m when r.m0 then 1 when r.m0 - 3 then 2 when r.m0 - 12 then 3 end as ordem
    from ref r join base b on b.m in (r.m0, r.m0 - 3, r.m0 - 12)
    
) q
order by edicao, ordem
