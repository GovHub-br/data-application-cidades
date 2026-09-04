{{ config(materialized='table') }}

-- Boletim de Conjuntura, página 2: Financiamentos Habitacionais (UH)
-- Seção do impresso: 3. Balanços das Empresas
--
-- Uma linha por EDIÇÃO (coluna `edicao`), com as colunas na ordem
-- impressa. O filtro do Superset seleciona a edição; o dashboard não
-- calcula nada — este SQL rodava como dataset virtual e voltava ao
-- engine a cada carregamento de página.

select "edicao", "periodo", "FGTS-PJ", "SBPE Const."
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
        select (right(periodo, 4)::int * 4 + left(periodo, 1)::int) as k,
               financ_hab_fgts_pj pj, financ_hab_sbpe_constr sb,
               financ_hab_fgts_pj_acumulado_12_meses pj12,
               financ_hab_sbpe_constr_acumulado_12_meses sb12
        from {{ ref('gld_financiamentos_habitacionais') }}
    )
    select e.edicao, x.rotulo as periodo, x.pj as "FGTS-PJ", x.sb as "SBPE Const.", x.ordem
    from edicoes e
    cross join lateral (
        select 'Trimestre selecionado' as rotulo, 1 as ordem, pj, sb from serie where k = e.k
        union all select 'Trimestre anterior', 2, pj, sb from serie where k = e.k - 1
        union all select 'Mesmo trim. do ano anterior', 3, pj, sb from serie where k = e.k - 4
        union all select '12 meses até a referência', 4, pj12, sb12 from serie where k = e.k
        union all select '12 meses anteriores', 5, pj12, sb12 from serie where k = e.k - 4
    ) x
    
) q
order by edicao, ordem
