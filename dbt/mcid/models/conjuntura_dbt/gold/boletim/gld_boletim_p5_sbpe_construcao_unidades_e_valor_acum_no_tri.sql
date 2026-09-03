{{ config(materialized='table') }}

-- Boletim de Conjuntura, página 5: SBPE Construção — unidades e valor (acum. no trimestre)
-- Seção do impresso: 7. SBPE Construção
--
-- Uma linha por EDIÇÃO (coluna `edicao`), com as colunas na ordem
-- impressa. O filtro do Superset seleciona a edição; o dashboard não
-- calcula nada — este SQL rodava como dataset virtual e voltava ao
-- engine a cada carregamento de página.
--
-- Fonte ABECIP automatizada. Conferido vs boletim 1T26: 47.609 un, R$ 11,22 bi, +149% e +83% — os quatro exatos.

select "edicao", "indicador", "Trim. ano anterior", "Trim. selecionado", "Variação %"
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
    select (ano * 12 + mes) as m, unidades_construcao u, valor_construcao_milhoes v
    from {{ ref('slv_abecip_financiamentos') }}
),
ref as (select edicao, ano_ed * 12 + tri_ed * 3 as m0 from edicoes)
select r.edicao, 'Unidades' as indicador,
       (select sum(u) from mes where m between r.m0 - 14 and r.m0 - 12) as "Trim. ano anterior",
       (select sum(u) from mes where m between r.m0 - 2 and r.m0)       as "Trim. selecionado",
       round(((select sum(u) from mes where m between r.m0 - 2 and r.m0)
            / nullif((select sum(u) from mes where m between r.m0 - 14 and r.m0 - 12), 0) - 1) * 100, 0) as "Variação %",
       1 as ordem
from ref r
union all
select r.edicao, 'Valor (R$ bilhões)',
       round(((select sum(v) from mes where m between r.m0 - 14 and r.m0 - 12) / 1000)::numeric, 2),
       round(((select sum(v) from mes where m between r.m0 - 2 and r.m0) / 1000)::numeric, 2),
       round(((select sum(v) from mes where m between r.m0 - 2 and r.m0)
            / nullif((select sum(v) from mes where m between r.m0 - 14 and r.m0 - 12), 0) - 1) * 100, 0),
       2
from ref r

) q
order by edicao, ordem
