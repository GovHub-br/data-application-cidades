{{ config(materialized='table') }}

-- Boletim de Conjuntura, página 1: CBIC — Lançamentos e Vendas (totais)
-- Seção do impresso: 2. Lançamentos e Vendas
--
-- Uma linha por EDIÇÃO (coluna `edicao`), com as colunas na ordem
-- impressa. O filtro do Superset seleciona a edição; o dashboard não
-- calcula nada — este SQL rodava como dataset virtual e voltava ao
-- engine a cada carregamento de página.
--
-- Inserção manual em lote (script 0003). A CBIC revisa trimestres já publicados.

select "edicao", "periodo", "Lançamentos TOTAL", "Lançamentos MCMV", "Lançamentos DEMAIS", "Vendas TOTAL", "Vendas MCMV", "Vendas DEMAIS"
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
    s as (
        select (right(periodo, 4)::int * 4 + left(periodo, 1)::int) as k,
               (case when btrim(cbic_lancamentos_total::text, E' \t\r\n\u00a0') ~ '^-?[0-9]*\.?[0-9]+([eE][-+]?[0-9]+)?$' then btrim(cbic_lancamentos_total::text, E' \t\r\n\u00a0')::numeric end) lt, (case when btrim(cbic_lancamentos_mcmv::text, E' \t\r\n\u00a0') ~ '^-?[0-9]*\.?[0-9]+([eE][-+]?[0-9]+)?$' then btrim(cbic_lancamentos_mcmv::text, E' \t\r\n\u00a0')::numeric end) lm,
               (case when btrim(cbic_vendas_total::text, E' \t\r\n\u00a0') ~ '^-?[0-9]*\.?[0-9]+([eE][-+]?[0-9]+)?$' then btrim(cbic_vendas_total::text, E' \t\r\n\u00a0')::numeric end) vt, (case when btrim(cbic_vendas_mcmv::text, E' \t\r\n\u00a0') ~ '^-?[0-9]*\.?[0-9]+([eE][-+]?[0-9]+)?$' then btrim(cbic_vendas_mcmv::text, E' \t\r\n\u00a0')::numeric end) vm,
               (case when btrim(cbic_lancamentos_total_acumulado_12_meses::text, E' \t\r\n\u00a0') ~ '^-?[0-9]*\.?[0-9]+([eE][-+]?[0-9]+)?$' then btrim(cbic_lancamentos_total_acumulado_12_meses::text, E' \t\r\n\u00a0')::numeric end) lt12,
               (case when btrim(cbic_lancamentos_mcmv_acumulado_12_meses::text, E' \t\r\n\u00a0') ~ '^-?[0-9]*\.?[0-9]+([eE][-+]?[0-9]+)?$' then btrim(cbic_lancamentos_mcmv_acumulado_12_meses::text, E' \t\r\n\u00a0')::numeric end) lm12,
               (case when btrim(cbic_vendas_total_acumulado_12_meses::text, E' \t\r\n\u00a0') ~ '^-?[0-9]*\.?[0-9]+([eE][-+]?[0-9]+)?$' then btrim(cbic_vendas_total_acumulado_12_meses::text, E' \t\r\n\u00a0')::numeric end) vt12,
               (case when btrim(cbic_vendas_mcmv_acumulado_12_meses::text, E' \t\r\n\u00a0') ~ '^-?[0-9]*\.?[0-9]+([eE][-+]?[0-9]+)?$' then btrim(cbic_vendas_mcmv_acumulado_12_meses::text, E' \t\r\n\u00a0')::numeric end) vm12
        from manual_conjuntura.dados_trimestrais
        where periodo ~ '^[1-4]T[0-9]{4}$'
    )
    select e.edicao, x.rotulo as periodo,
           x.lt as "Lançamentos TOTAL", x.lm as "Lançamentos MCMV", x.lt - x.lm as "Lançamentos DEMAIS",
           x.vt as "Vendas TOTAL", x.vm as "Vendas MCMV", x.vt - x.vm as "Vendas DEMAIS", x.ordem
    from edicoes e
    cross join lateral (
        select 'Trimestre selecionado' as rotulo, 1 as ordem, lt, lm, vt, vm from s where k = e.k
        union all select 'Trimestre anterior', 2, lt, lm, vt, vm from s where k = e.k - 1
        union all select 'Mesmo trim. do ano anterior', 3, lt, lm, vt, vm from s where k = e.k - 4
        union all select '12 meses até a referência', 4, lt12, lm12, vt12, vm12 from s where k = e.k
        union all select '12 meses anteriores', 5, lt12, lm12, vt12, vm12 from s where k = e.k - 4
    ) x
    
) q
order by edicao, ordem
