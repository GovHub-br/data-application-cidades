{{ config(materialized='table') }}

-- Boletim de Conjuntura, página 1: PIB Construção Civil (em % de Crescimento)
-- Seção do impresso: 1. PIB da Construção Civil
--
-- Uma linha por EDIÇÃO (coluna `edicao`), com as colunas na ordem
-- impressa. O filtro do Superset seleciona a edição; o dashboard não
-- calcula nada — este SQL rodava como dataset virtual e voltava ao
-- engine a cada carregamento de página.

select "edicao", "indicador", "4 trim. antes", "3 trim. antes", "2 trim. antes", "trim. anterior", "trimestre selecionado"
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
        select (right(periodo, 4)::int * 4 + left(periodo, 1)::int) as k,
               var_trim_trim_anterior      as v1,
               var_acumulada_ano           as v2,
               var_acumulada_4_trimestres  as v3
        from {{ ref('gold_continuo_pib_construcao_civil_pct') }}
    ),
    m as (
        select e.edicao, e.k,
               1 as ordem, 'Trim./Trim. Imediatamente Anterior' as indicador,
               (select v1 from serie where k = e.k - 4) as c4,
               (select v1 from serie where k = e.k - 3) as c3,
               (select v1 from serie where k = e.k - 2) as c2,
               (select v1 from serie where k = e.k - 1) as c1,
               (select v1 from serie where k = e.k)     as c0
        from edicoes e
        union all
        select e.edicao, e.k, 2, 'Acumulada ao Longo do Ano',
               (select v2 from serie where k = e.k - 4), (select v2 from serie where k = e.k - 3),
               (select v2 from serie where k = e.k - 2), (select v2 from serie where k = e.k - 1),
               (select v2 from serie where k = e.k)
        from edicoes e
        union all
        select e.edicao, e.k, 3, 'Acum. Últimos 4 Trimestres',
               (select v3 from serie where k = e.k - 4), (select v3 from serie where k = e.k - 3),
               (select v3 from serie where k = e.k - 2), (select v3 from serie where k = e.k - 1),
               (select v3 from serie where k = e.k)
        from edicoes e
    )
    select edicao, indicador,
           c4 as "4 trim. antes", c3 as "3 trim. antes", c2 as "2 trim. antes",
           c1 as "trim. anterior", c0 as "trimestre selecionado", ordem
    from m
    
) q
order by edicao, ordem
