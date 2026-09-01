{{ config(materialized='table') }}

-- Boletim de Conjuntura, página 2: Financiamentos Imobiliários (BACEN)
-- Seção do impresso: 3. Balanços das Empresas
--
-- Uma linha por EDIÇÃO (coluna `edicao`), com as colunas na ordem
-- impressa. O filtro do Superset seleciona a edição; o dashboard não
-- calcula nada — este SQL rodava como dataset virtual e voltava ao
-- engine a cada carregamento de página.

select "edicao", "periodo", "PF Concessões (R$ mi)", "PF Taxa de Juros (%a.a)", "PF Inadimplência (%)", "PJ Concessões (R$ mi)", "PJ Taxa de Juros (%a.a)", "PJ Inadimplência (%)"
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
        select (extract(year from data)::int * 12 + extract(month from data)::int) as m,
               concessoes_pf_rs_mi pf, taxa_juros_pf_aa tpf, inadimplencia_pf_pct ipf,
               concessoes_pj_rs_mi pj, taxa_juros_pj_aa tpj, inadimplencia_pj_pct ipj
        from {{ ref('gold_continuo_financiamentos_imobiliarios_pf_pj') }}
    ),
    ref as (select edicao, k, ano_ed * 12 + tri_ed * 3 as m0 from edicoes)
    select r.edicao, x.rotulo as periodo,
           round(x.pf::numeric, 0) as "PF Concessões (R$ mi)",
           round(x.tpf::numeric, 1) as "PF Taxa de Juros (%a.a)",
           round(x.ipf::numeric, 1) as "PF Inadimplência (%)",
           round(x.pj::numeric, 0) as "PJ Concessões (R$ mi)",
           round(x.tpj::numeric, 1) as "PJ Taxa de Juros (%a.a)",
           round(x.ipj::numeric, 1) as "PJ Inadimplência (%)",
           x.ordem
    from ref r
    cross join lateral (
        select 'Mês de referência' as rotulo, 1 as ordem, pf, tpf, ipf, pj, tpj, ipj from mes where m = r.m0
        union all
        select 'Mês anterior', 2, pf, tpf, ipf, pj, tpj, ipj from mes where m = r.m0 - 1
        union all
        select 'Mesmo mês do ano anterior', 3, pf, tpf, ipf, pj, tpj, ipj from mes where m = r.m0 - 12
        union all
        select '12 meses até a referência', 4, sum(pf), null, null, sum(pj), null, null
        from mes where m between r.m0 - 11 and r.m0
        union all
        select '12 meses anteriores', 5, sum(pf), null, null, sum(pj), null, null
        from mes where m between r.m0 - 23 and r.m0 - 12
    ) x
    
) q
order by edicao, ordem
