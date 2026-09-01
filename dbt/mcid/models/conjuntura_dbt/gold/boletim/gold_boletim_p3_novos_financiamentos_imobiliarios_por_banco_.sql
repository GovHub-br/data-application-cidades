{{ config(materialized='table') }}

-- Boletim de Conjuntura, página 3: Novos Financiamentos Imobiliários por Banco (acum. no ano)
-- Seção do impresso: 6. Crédito
--
-- Uma linha por EDIÇÃO (coluna `edicao`), com as colunas na ordem
-- impressa. O filtro do Superset seleciona a edição; o dashboard não
-- calcula nada — este SQL rodava como dataset virtual e voltava ao
-- engine a cada carregamento de página.
--
-- Tabela única: ABECIP automatizada onde existe, planilha manual no histórico.

select "edicao", "banco", "UH acum. ano", "R$ bi acum. ano", "% UH", "fonte"
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
    ref as (select edicao, ano_ed, tri_ed * 3 as mes_ed from edicoes)
    select r.edicao, g.instituicao as banco,
           g.uh_acumulado_ano as "UH acum. ano",
           round((g.volume_acumulado_ano_milhoes / 1000)::numeric, 1) as "R$ bi acum. ano",
           round((g.uh_participacao * 100)::numeric, 1) as "% UH",
           g.fonte as "fonte",
           case when g.instituicao = 'TOTAL' then 0 else 1 end as ordem_grupo,
           coalesce(g.uh_acumulado_ano, 0) as ordem
    from ref r
    join {{ ref('gold_continuo_financiamentos_instituicao') }} g
      on g.ano = r.ano_ed and g.mes = r.mes_ed
    
) q
order by edicao, ordem_grupo, ordem desc
