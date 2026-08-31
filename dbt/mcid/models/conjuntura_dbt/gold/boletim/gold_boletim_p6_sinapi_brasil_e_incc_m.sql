{{ config(materialized='table') }}

-- Boletim de Conjuntura, página 6: SINAPI (Brasil) e INCC-M
-- Seção do impresso: 7. Preços
--
-- Uma linha por EDIÇÃO (coluna `edicao`), com as colunas na ordem
-- impressa. O filtro do Superset seleciona a edição; o dashboard não
-- calcula nada — este SQL rodava como dataset virtual e voltava ao
-- engine a cada carregamento de página.
--
-- INCC-M: as colunas de acumulado da fonte estão trocadas — ver relatório.

select "edicao", "indicador", "SINAPI", "INCC-M"
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
    sin as (
        select (left(periodo, 4)::int * 12 + right(periodo, 2)::int) as m,
               custo_medio_m2 ix, var_mes vm, var_acum_ano va, var_12_meses vd
        from {{ ref('gold_continuo_sinapi') }}
    ),
    inc as (
        select (extract(year from mes)::int * 12 + extract(month from mes)::int) as m,
               indice ix, var_mes vm, var_fonte_no_ano va, var_fonte_12_meses vd
        from {{ ref('gold_continuo_incc_m') }}
    ),
    ref as (select edicao, ano_ed * 12 + tri_ed * 3 as m0 from edicoes)
    select r.edicao, 'Número índice / Custo (R$/m²)' as indicador,
           round((select ix from sin where m = r.m0)::numeric, 1) as "SINAPI",
           round((select ix from inc where m = r.m0)::numeric, 2) as "INCC-M", 1 as ordem
    from ref r
    union all
    select r.edicao, 'Variação mensal (%)',
           round((select vm from sin where m = r.m0)::numeric, 2),
           round((select vm from inc where m = r.m0)::numeric, 2), 2
    from ref r
    union all
    select r.edicao, 'Acumulado no ano (%)',
           round((select va from sin where m = r.m0)::numeric, 2),
           round((select va from inc where m = r.m0)::numeric, 2), 3
    from ref r
    union all
    select r.edicao, 'Acumulado em 12 meses (%)',
           round((select vd from sin where m = r.m0)::numeric, 2),
           round((select vd from inc where m = r.m0)::numeric, 2), 4
    from ref r
    
) q
order by edicao, ordem
