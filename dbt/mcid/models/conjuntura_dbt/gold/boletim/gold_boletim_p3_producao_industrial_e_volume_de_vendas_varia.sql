{{ config(materialized='table') }}

-- Boletim de Conjuntura, página 3: Produção Industrial e Volume de Vendas (variação %)
-- Seção do impresso: 5. Produção Física Industrial e Vendas da Construção
--
-- Uma linha por EDIÇÃO (coluna `edicao`), com as colunas na ordem
-- impressa. O filtro do Superset seleciona a edição; o dashboard não
-- calcula nada — este SQL rodava como dataset virtual e voltava ao
-- engine a cada carregamento de página.

select "edicao", "indicador", "PROD mesmo mês ano ant.", "PROD mês anterior", "PROD mês de referência", "VENDAS mesmo mês ano ant.", "VENDAS mês anterior", "VENDAS mês de referência"
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
    mes as (
        select (left(periodo, 4)::int * 12 + right(periodo, 2)::int) as m,
               pim_pf_var_mes pm, pim_pf_var_acum_ano pa, pim_pf_var_12_meses pd,
               pmc_var_mes vm, pmc_var_acum_ano va, pmc_var_12_meses vd
        from {{ ref('gold_continuo_producao_fisica') }}
    ),
    ref as (select edicao, ano_ed * 12 + tri_ed * 3 as m0 from edicoes)
    select r.edicao, 'Variação percentual mensal' as indicador,
           (select pm from mes where m = r.m0 - 12) as "PROD mesmo mês ano ant.",
           (select pm from mes where m = r.m0 - 1)  as "PROD mês anterior",
           (select pm from mes where m = r.m0)      as "PROD mês de referência",
           (select vm from mes where m = r.m0 - 12) as "VENDAS mesmo mês ano ant.",
           (select vm from mes where m = r.m0 - 1)  as "VENDAS mês anterior",
           (select vm from mes where m = r.m0)      as "VENDAS mês de referência",
           1 as ordem
    from ref r
    union all
    select r.edicao, 'Variação percentual acumulada no ano',
           (select pa from mes where m = r.m0 - 12), (select pa from mes where m = r.m0 - 1),
           (select pa from mes where m = r.m0), (select va from mes where m = r.m0 - 12),
           (select va from mes where m = r.m0 - 1), (select va from mes where m = r.m0), 2
    from ref r
    union all
    select r.edicao, 'Variação percentual acumulada nos últimos 12 meses',
           (select pd from mes where m = r.m0 - 12), (select pd from mes where m = r.m0 - 1),
           (select pd from mes where m = r.m0), (select vd from mes where m = r.m0 - 12),
           (select vd from mes where m = r.m0 - 1), (select vd from mes where m = r.m0), 3
    from ref r
    
) q
order by edicao, ordem
