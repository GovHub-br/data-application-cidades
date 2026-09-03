{{ config(materialized='table') }}

-- Boletim de Conjuntura, página 2: Vendas por construtora (variação %)
-- Seção do impresso: 3. Balanços das Empresas
--
-- Uma linha por EDIÇÃO (coluna `edicao`), com as colunas na ordem
-- impressa. O filtro do Superset seleciona a edição; o dashboard não
-- calcula nada — este SQL rodava como dataset virtual e voltava ao
-- engine a cada carregamento de página.

select "edicao", "empresa", "vs. trim. anterior", "vs. mesmo trim. ano ant.", "12m atual / 12m anterior", "12m anterior / 12m retrasado"
from (

    with 
edicoes as (
    select
        (extract(quarter from t)::int::text || 'T'
         || extract(year from t)::int::text)                as edicao,
        (extract(year from t)::int * 4
         + extract(quarter from t)::int)                    as k,
        extract(year from t)::int                           as ano_ed,
        extract(quarter from t)::int                        as tri_ed
    from generate_series(
        make_date(2025, 1, 1),
        date_trunc('quarter', current_date)::date,
        interval '3 months'
    ) as t
),
    serie as (
        select empresa, (right(periodo, 4)::int * 4 + left(periodo, 1)::int) as k,
               vendas::numeric as v,
               (case when btrim(var_vendas_tri_anterior::text, E' \t\r\n\u00a0') ~ '^-?[0-9]*\.?[0-9]+([eE][-+]?[0-9]+)?$' then btrim(var_vendas_tri_anterior::text, E' \t\r\n\u00a0')::numeric end)            as var_tri,
               (case when btrim(var_vendas_mesmo_tri_ano_anterior::text, E' \t\r\n\u00a0') ~ '^-?[0-9]*\.?[0-9]+([eE][-+]?[0-9]+)?$' then btrim(var_vendas_mesmo_tri_ano_anterior::text, E' \t\r\n\u00a0')::numeric end)  as var_ano
        from {{ ref('gld_balancos_empresas') }}
    )
    select e.edicao, s.empresa,
           round(s.var_tri * 100, 0) as "vs. trim. anterior",
           round(s.var_ano * 100, 0) as "vs. mesmo trim. ano ant.",
           round((( select sum(v) from serie x where x.empresa = s.empresa and x.k between e.k - 3 and e.k)
                / nullif((select sum(v) from serie x where x.empresa = s.empresa and x.k between e.k - 7 and e.k - 4), 0) - 1) * 100, 0)
                as "12m atual / 12m anterior",
           round((( select sum(v) from serie x where x.empresa = s.empresa and x.k between e.k - 7 and e.k - 4)
                / nullif((select sum(v) from serie x where x.empresa = s.empresa and x.k between e.k - 11 and e.k - 8), 0) - 1) * 100, 0)
                as "12m anterior / 12m retrasado",
           case s.empresa when 'MRV' then 1 when 'Cury' then 2 when 'Tenda' then 3
                when 'Direcional' then 4 when 'Pacaembu' then 5 else 6 end as ordem
    from edicoes e join serie s on s.k = e.k
    
) q
order by edicao, ordem
