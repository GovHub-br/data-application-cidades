{{ config(materialized='table') }}

-- Boletim de Conjuntura, página 4: Nº UH por Condição de Uso
-- Seção do impresso: 6. Crédito
--
-- Uma linha por EDIÇÃO (coluna `edicao`), com as colunas na ordem
-- impressa. O filtro do Superset seleciona a edição; o dashboard não
-- calcula nada — este SQL rodava como dataset virtual e voltava ao
-- engine a cada carregamento de página.

select "edicao", "fonte", "Trim. ano anterior — UH Usadas", "Trim. ano anterior — UH Novas", "Trim. selecionado — UH Usadas", "Trim. selecionado — UH Novas", "Trim. selecionado — UH Total"
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
    mes as (
        select (extract(year from data_referencia)::int * 12 + extract(month from data_referencia)::int) as m,
               fgts_pf_uh_usados fu, fgts_pf_uh_novos fn,
               abecip_sbpe_fin_uh_aq_usados su, abecip_sbpe_fin_uh_aq_novos sn,
               abecip_sbpe_fin_uh_aq_total st
        from {{ ref('gold_continuo_uh_condicao_uso') }}
    ),
    ref as (select edicao, ano_ed * 12 + tri_ed * 3 as m0 from edicoes)
    select r.edicao, 'FGTS - PF' as fonte,
           (select sum(fu) from mes where m between r.m0 - 14 and r.m0 - 12) as "Trim. ano anterior — UH Usadas",
           (select sum(fn) from mes where m between r.m0 - 14 and r.m0 - 12) as "Trim. ano anterior — UH Novas",
           (select sum(fu) from mes where m between r.m0 - 2 and r.m0)       as "Trim. selecionado — UH Usadas",
           (select sum(fn) from mes where m between r.m0 - 2 and r.m0)       as "Trim. selecionado — UH Novas",
           null::numeric as "Trim. selecionado — UH Total",
           1 as ordem
    from ref r
    union all
    select r.edicao, 'SBPE (Aquisição)',
           (select sum(su) from mes where m between r.m0 - 14 and r.m0 - 12),
           (select sum(sn) from mes where m between r.m0 - 14 and r.m0 - 12),
           (select sum(su) from mes where m between r.m0 - 2 and r.m0),
           (select sum(sn) from mes where m between r.m0 - 2 and r.m0),
           (select sum(st) from mes where m between r.m0 - 2 and r.m0), 2
    from ref r
    
) q
order by edicao, ordem
