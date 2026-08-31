{{ config(materialized='table') }}

-- Boletim de Conjuntura, página 5: Saldo Caderneta de Poupança — Captação Líquida (R$ bi)
-- Seção do impresso: 7. Poupança
--
-- Uma linha por EDIÇÃO (coluna `edicao`), com as colunas na ordem
-- impressa. O filtro do Superset seleciona a edição; o dashboard não
-- calcula nada — este SQL rodava como dataset virtual e voltava ao
-- engine a cada carregamento de página.

select "edicao", "periodo", "Cap. Líq. (Bi)"
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
               captacao_liquida_valor v
        from {{ ref('gold_continuo_saldo_poupanca') }}
    ),
    ref as (select edicao, ano_ed * 12 + tri_ed * 3 as m0 from edicoes)
    select r.edicao, x.rotulo as periodo,
           round((x.v / 1000)::numeric, 1) as "Cap. Líq. (Bi)", x.ordem
    from ref r
    cross join lateral (
        select 'Mês de referência' as rotulo, 1 as ordem, v from mes where m = r.m0
        union all select 'Mês anterior', 2, v from mes where m = r.m0 - 1
        union all select 'Mesmo mês do ano anterior', 3, v from mes where m = r.m0 - 12
        union all select '12 meses até a referência', 4, sum(v) from mes where m between r.m0 - 11 and r.m0
        union all select '12 meses anteriores', 5, sum(v) from mes where m between r.m0 - 23 and r.m0 - 12
    ) x
    
) q
order by edicao, ordem
