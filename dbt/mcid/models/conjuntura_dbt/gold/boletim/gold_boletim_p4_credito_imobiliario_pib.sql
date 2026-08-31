{{ config(materialized='table') }}

-- Boletim de Conjuntura, página 4: Crédito Imobiliário / PIB (%)
-- Seção do impresso: 6. Crédito
--
-- Uma linha por EDIÇÃO (coluna `edicao`), com as colunas na ordem
-- impressa. O filtro do Superset seleciona a edição; o dashboard não
-- calcula nada — este SQL rodava como dataset virtual e voltava ao
-- engine a cada carregamento de página.
--
-- O boletim mostra 16 meses encerrados no mês de referência.

select "edicao", "periodo", "Crédito Imobiliário / PIB"
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
        select (extract(year from data)::int * 12 + extract(month from data)::int) as m,
               to_char(data, 'MM/YY') as rotulo, credito_imobiliario_pib_pct pct
        from {{ ref('gold_continuo_credito_pib') }}
    ),
    ref as (select edicao, ano_ed * 12 + tri_ed * 3 as m0 from edicoes)
    select r.edicao, x.rotulo as periodo,
           round(x.pct::numeric, 2) as "Crédito Imobiliário / PIB", x.m as ordem
    from ref r join mes x on x.m between r.m0 - 15 and r.m0
    
) q
order by edicao, ordem
