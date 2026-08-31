{{ config(materialized='table') }}

-- Boletim de Conjuntura, página 3: Empregos Construção (CAGED)
-- Seção do impresso: 4. Empregos
--
-- Uma linha por EDIÇÃO (coluna `edicao`), com as colunas na ordem
-- impressa. O filtro do Superset seleciona a edição; o dashboard não
-- calcula nada — este SQL rodava como dataset virtual e voltava ao
-- engine a cada carregamento de página.

select "edicao", "periodo", "Criação Líquida (Saldo)", "Total de Postos (Estoque)"
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
               total_construcao_saldo saldo, total_construcao_estoque estoque
        from {{ ref('gold_continuo_empregos_caged') }}
    ),
    ref as (select edicao, ano_ed * 12 + tri_ed * 3 as m0 from edicoes)
    select r.edicao, x.rotulo as periodo,
           x.saldo as "Criação Líquida (Saldo)", x.estoque as "Total de Postos (Estoque)", x.ordem
    from ref r
    cross join lateral (
        select 'Mês de referência' as rotulo, 1 as ordem, saldo, estoque from mes where m = r.m0
        union all select 'Mês anterior', 2, saldo, estoque from mes where m = r.m0 - 1
        union all select 'Mesmo mês do ano anterior', 3, saldo, estoque from mes where m = r.m0 - 12
        union all select 'Acumulado no trimestre', 4, sum(saldo), null from mes where m between r.m0 - 2 and r.m0
        union all select 'Acum. no trim. do ano anterior', 5, sum(saldo), null from mes where m between r.m0 - 14 and r.m0 - 12
    ) x
    
) q
order by edicao, ordem
