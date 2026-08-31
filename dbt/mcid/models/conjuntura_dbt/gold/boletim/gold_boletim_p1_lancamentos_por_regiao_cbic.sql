{{ config(materialized='table') }}

-- Boletim de Conjuntura, página 1: Lançamentos por Região (CBIC)
-- Seção do impresso: 2. Lançamentos e Vendas
--
-- Uma linha por EDIÇÃO (coluna `edicao`), com as colunas na ordem
-- impressa. O filtro do Superset seleciona a edição; o dashboard não
-- calcula nada — este SQL rodava como dataset virtual e voltava ao
-- engine a cada carregamento de página.

select "edicao", "regiao", "TOTAL", "MCMV", "% MCMV"
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
    d as (select periodo, periodo from manual_conjuntura.dados_trimestrais)
    select e.edicao, x.regiao, x.total as "TOTAL", x.mcmv as "MCMV",
           round((x.mcmv / nullif(x.total, 0) * 100)::numeric, 0) as "% MCMV", x.ordem
    from edicoes e
    join manual_conjuntura.dados_trimestrais d on d.periodo = e.edicao
    cross join lateral (
        select 'NORTE' as regiao, 1 as ordem, (case when btrim(d.cbic_lancamentos_total_n::text, E' \t\r\n\u00a0') ~ '^-?[0-9]*\.?[0-9]+([eE][-+]?[0-9]+)?$' then btrim(d.cbic_lancamentos_total_n::text, E' \t\r\n\u00a0')::numeric end) total, (case when btrim(d.cbic_lancamentos_mcmv_n::text, E' \t\r\n\u00a0') ~ '^-?[0-9]*\.?[0-9]+([eE][-+]?[0-9]+)?$' then btrim(d.cbic_lancamentos_mcmv_n::text, E' \t\r\n\u00a0')::numeric end) mcmv
        union all select 'NORDESTE', 2, (case when btrim(d.cbic_lancamentos_total_ne::text, E' \t\r\n\u00a0') ~ '^-?[0-9]*\.?[0-9]+([eE][-+]?[0-9]+)?$' then btrim(d.cbic_lancamentos_total_ne::text, E' \t\r\n\u00a0')::numeric end), (case when btrim(d.cbic_lancamentos_mcmv_ne::text, E' \t\r\n\u00a0') ~ '^-?[0-9]*\.?[0-9]+([eE][-+]?[0-9]+)?$' then btrim(d.cbic_lancamentos_mcmv_ne::text, E' \t\r\n\u00a0')::numeric end)
        union all select 'CENTRO-OESTE', 3, (case when btrim(d.cbic_lancamentos_total_co::text, E' \t\r\n\u00a0') ~ '^-?[0-9]*\.?[0-9]+([eE][-+]?[0-9]+)?$' then btrim(d.cbic_lancamentos_total_co::text, E' \t\r\n\u00a0')::numeric end), (case when btrim(d.cbic_lancamentos_mcmv_co::text, E' \t\r\n\u00a0') ~ '^-?[0-9]*\.?[0-9]+([eE][-+]?[0-9]+)?$' then btrim(d.cbic_lancamentos_mcmv_co::text, E' \t\r\n\u00a0')::numeric end)
        union all select 'SUDESTE', 4, (case when btrim(d.cbic_lancamentos_total_se::text, E' \t\r\n\u00a0') ~ '^-?[0-9]*\.?[0-9]+([eE][-+]?[0-9]+)?$' then btrim(d.cbic_lancamentos_total_se::text, E' \t\r\n\u00a0')::numeric end), (case when btrim(d.cbic_lancamentos_mcmv_se::text, E' \t\r\n\u00a0') ~ '^-?[0-9]*\.?[0-9]+([eE][-+]?[0-9]+)?$' then btrim(d.cbic_lancamentos_mcmv_se::text, E' \t\r\n\u00a0')::numeric end)
        union all select 'SUL', 5, (case when btrim(d.cbic_lancamentos_total_s::text, E' \t\r\n\u00a0') ~ '^-?[0-9]*\.?[0-9]+([eE][-+]?[0-9]+)?$' then btrim(d.cbic_lancamentos_total_s::text, E' \t\r\n\u00a0')::numeric end), (case when btrim(d.cbic_lancamentos_mcmv_s::text, E' \t\r\n\u00a0') ~ '^-?[0-9]*\.?[0-9]+([eE][-+]?[0-9]+)?$' then btrim(d.cbic_lancamentos_mcmv_s::text, E' \t\r\n\u00a0')::numeric end)
    ) x
    
) q
order by edicao, ordem
