{{ config(materialized='table') }}

-- Boletim de Conjuntura, página 2: Totais das empresas levantadas (variação %)
-- Seção do impresso: 3. Balanços das Empresas
--
-- Uma linha por EDIÇÃO (coluna `edicao`), com as colunas na ordem
-- impressa. O filtro do Superset seleciona a edição; o dashboard não
-- calcula nada — este SQL rodava como dataset virtual e voltava ao
-- engine a cada carregamento de página.

select "edicao", "indicador", "vs. trim. anterior", "vs. mesmo trim. ano ant.", "12m atual / 12m anterior", "12m anterior / 12m retrasado"
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
    soma as (
        select (right(periodo, 4)::int * 4 + left(periodo, 1)::int) as k,
               sum(lancamentos::numeric) as lv, sum(vendas::numeric) as vv
        from {{ ref('gold_continuo_balancos_empresas') }} group by 1
    ),
    tot as (
        select (right(periodo, 4)::int * 4 + left(periodo, 1)::int) as k,
               (case when btrim(var_lancamentos_totais_tri_anterior::text, E' \t\r\n\u00a0') ~ '^-?[0-9]*\.?[0-9]+([eE][-+]?[0-9]+)?$' then btrim(var_lancamentos_totais_tri_anterior::text, E' \t\r\n\u00a0')::numeric end)           as lt,
               (case when btrim(var_lancamentos_totais_mesmo_tri_ano_anterior::text, E' \t\r\n\u00a0') ~ '^-?[0-9]*\.?[0-9]+([eE][-+]?[0-9]+)?$' then btrim(var_lancamentos_totais_mesmo_tri_ano_anterior::text, E' \t\r\n\u00a0')::numeric end) as la,
               (case when btrim(var_vendas_totais_tri_anterior::text, E' \t\r\n\u00a0') ~ '^-?[0-9]*\.?[0-9]+([eE][-+]?[0-9]+)?$' then btrim(var_vendas_totais_tri_anterior::text, E' \t\r\n\u00a0')::numeric end)                as vt,
               (case when btrim(var_vendas_totais_mesmo_tri_ano_anterior::text, E' \t\r\n\u00a0') ~ '^-?[0-9]*\.?[0-9]+([eE][-+]?[0-9]+)?$' then btrim(var_vendas_totais_mesmo_tri_ano_anterior::text, E' \t\r\n\u00a0')::numeric end)      as va
        from {{ ref('gold_continuo_balancos_empresas_totais') }}
    )
    select e.edicao, 'Total lançamentos' as indicador,
           round(t.lt * 100, 0) as "vs. trim. anterior",
           round(t.la * 100, 0) as "vs. mesmo trim. ano ant.",
           round((( select sum(lv) from soma x where x.k between e.k - 3 and e.k)
                / nullif((select sum(lv) from soma x where x.k between e.k - 7 and e.k - 4), 0) - 1) * 100, 0) as "12m atual / 12m anterior",
           round((( select sum(lv) from soma x where x.k between e.k - 7 and e.k - 4)
                / nullif((select sum(lv) from soma x where x.k between e.k - 11 and e.k - 8), 0) - 1) * 100, 0) as "12m anterior / 12m retrasado",
           1 as ordem
    from edicoes e join tot t on t.k = e.k
    union all
    select e.edicao, 'Total vendas',
           round(t.vt * 100, 0), round(t.va * 100, 0),
           round((( select sum(vv) from soma x where x.k between e.k - 3 and e.k)
                / nullif((select sum(vv) from soma x where x.k between e.k - 7 and e.k - 4), 0) - 1) * 100, 0),
           round((( select sum(vv) from soma x where x.k between e.k - 7 and e.k - 4)
                / nullif((select sum(vv) from soma x where x.k between e.k - 11 and e.k - 8), 0) - 1) * 100, 0),
           2
    from edicoes e join tot t on t.k = e.k
    
) q
order by edicao, ordem
