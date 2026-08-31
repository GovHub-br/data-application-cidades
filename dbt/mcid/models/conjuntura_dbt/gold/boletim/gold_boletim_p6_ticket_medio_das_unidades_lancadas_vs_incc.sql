{{ config(materialized='table') }}

-- Boletim de Conjuntura, página 6: Ticket médio das unidades lançadas vs. INCC
-- Seção do impresso: 7. Preços
--
-- Uma linha por EDIÇÃO (coluna `edicao`), com as colunas na ordem
-- impressa. O filtro do Superset seleciona a edição; o dashboard não
-- calcula nada — este SQL rodava como dataset virtual e voltava ao
-- engine a cada carregamento de página.
--
-- O boletim mostra 9 trimestres encerrados na edição selecionada.

select "edicao", "periodo", "INCC trimestral", "MRV trimestral", "Direcional trimestral", "Tenda trimestral", "INCC acum. 4T20", "MRV acum. 4T20", "Direcional acum. 4T20", "Tenda acum. 4T20"
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
    serie as (
        select (right(periodo, 4)::int * 4 + left(periodo, 1)::int) as k, periodo,
               incc_var_tri_ant it, incc_var_acum_4t2020 ia,
               ticket_medio_lancamentos_mrv_var_tri_ant mt, ticket_medio_lancamentos_mrv_var_acum_4t2020 ma,
               ticket_medio_lancamentos_direcional_var_tri_ant dt, ticket_medio_lancamentos_direcional_var_acum_4t2020 da,
               ticket_medio_lancamentos_tenda_var_tri_ant tt, ticket_medio_lancamentos_tenda_var_acum_4t2020 ta
        from {{ ref('gold_continuo_ticket_medio') }}
    )
    select e.edicao, s.periodo,
           round(s.it::numeric * 100, 1) as "INCC trimestral",
           round(s.mt::numeric * 100, 1) as "MRV trimestral",
           round(s.dt::numeric * 100, 1) as "Direcional trimestral",
           round(s.tt::numeric * 100, 1) as "Tenda trimestral",
           round(s.ia::numeric * 100, 1) as "INCC acum. 4T20",
           round(s.ma::numeric * 100, 1) as "MRV acum. 4T20",
           round(s.da::numeric * 100, 1) as "Direcional acum. 4T20",
           round(s.ta::numeric * 100, 1) as "Tenda acum. 4T20",
           s.k as ordem
    from edicoes e join serie s on s.k between e.k - 8 and e.k
    
) q
order by edicao, ordem
