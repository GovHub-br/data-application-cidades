{{ config(materialized='table') }}

-- Boletim de Conjuntura, página 5: Financiamento PF MCMV por faixa
-- Seção do impresso: 7. Financiamento PF
--
-- Uma linha por EDIÇÃO (coluna `edicao`), com as colunas na ordem
-- impressa. O filtro do Superset seleciona a edição; o dashboard não
-- calcula nada — este SQL rodava como dataset virtual e voltava ao
-- engine a cada carregamento de página.

select "edicao", "faixa", "Trim. ano anterior — Nº UH", "Trim. ano anterior — FIN (Bi R$)", "Trim. selecionado — Nº UH", "Trim. selecionado — FIN (Bi R$)"
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
        select (extract(year from data_referencia)::int * 12 + extract(month from data_referencia)::int) as m,
               financiamento_pf_uh_faixa_1 u1, financiamento_pf_valor_faixa_1 v1,
               financiamento_pf_uh_faixa_2 u2, financiamento_pf_valor_faixa_2 v2,
               financiamento_pf_uh_faixa_3 u3, financiamento_pf_valor_faixa_3 v3,
               financiamento_pf_uh_classe_media uc, financiamento_pf_valor_classe_media vc,
               financiamento_pf_uh_total ut, financiamento_pf_valor_total vt
        from {{ ref('gld_financiamento_pf_faixa') }}
    ),
    ref as (select edicao, ano_ed * 12 + tri_ed * 3 as m0 from edicoes),
    f as (
        select 'Faixa 1' as faixa, 1 as ordem, 'u1' as cu, 'v1' as cv union all
        select 'Faixa 2', 2, 'u2', 'v2' union all
        select 'Faixa 3', 3, 'u3', 'v3' union all
        select 'Faixa Classe Média', 4, 'uc', 'vc' union all
        select 'TOTAL', 9, 'ut', 'vt'
    )
    select r.edicao, x.faixa,
           x.ua as "Trim. ano anterior — Nº UH",
           round((x.va / 1e9)::numeric, 2) as "Trim. ano anterior — FIN (Bi R$)",
           x.ub as "Trim. selecionado — Nº UH",
           round((x.vb / 1e9)::numeric, 2) as "Trim. selecionado — FIN (Bi R$)",
           x.ordem
    from ref r
    cross join lateral (
        select 'Faixa 1' as faixa, 1 as ordem,
               (select sum(u1) from mes where m between r.m0 - 14 and r.m0 - 12) ua,
               (select sum(v1) from mes where m between r.m0 - 14 and r.m0 - 12) va,
               (select sum(u1) from mes where m between r.m0 - 2 and r.m0) ub,
               (select sum(v1) from mes where m between r.m0 - 2 and r.m0) vb
        union all
        select 'Faixa 2', 2,
               (select sum(u2) from mes where m between r.m0 - 14 and r.m0 - 12),
               (select sum(v2) from mes where m between r.m0 - 14 and r.m0 - 12),
               (select sum(u2) from mes where m between r.m0 - 2 and r.m0),
               (select sum(v2) from mes where m between r.m0 - 2 and r.m0)
        union all
        select 'Faixa 3', 3,
               (select sum(u3) from mes where m between r.m0 - 14 and r.m0 - 12),
               (select sum(v3) from mes where m between r.m0 - 14 and r.m0 - 12),
               (select sum(u3) from mes where m between r.m0 - 2 and r.m0),
               (select sum(v3) from mes where m between r.m0 - 2 and r.m0)
        union all
        select 'Faixa Classe Média', 4,
               (select sum(uc) from mes where m between r.m0 - 14 and r.m0 - 12),
               (select sum(vc) from mes where m between r.m0 - 14 and r.m0 - 12),
               (select sum(uc) from mes where m between r.m0 - 2 and r.m0),
               (select sum(vc) from mes where m between r.m0 - 2 and r.m0)
        union all
        select 'TOTAL', 9,
               (select sum(ut) from mes where m between r.m0 - 14 and r.m0 - 12),
               (select sum(vt) from mes where m between r.m0 - 14 and r.m0 - 12),
               (select sum(ut) from mes where m between r.m0 - 2 and r.m0),
               (select sum(vt) from mes where m between r.m0 - 2 and r.m0)
    ) x
    
) q
order by edicao, ordem
