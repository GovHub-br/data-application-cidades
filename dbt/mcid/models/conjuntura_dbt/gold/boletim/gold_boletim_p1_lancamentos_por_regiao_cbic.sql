{{ config(materialized='table') }}

-- Boletim de Conjuntura, página 1: Lançamentos por Região (CBIC)
-- Seção do impresso: 2. Lançamentos e Vendas
--
-- Uma linha por EDIÇÃO (coluna `edicao`), com as colunas na ordem
-- impressa. O filtro do Superset seleciona a edição; o dashboard não
-- calcula nada.
--
-- Lê a tabela controlada do CBIC, não a planilha manual. As duas guardavam
-- a mesma abertura por região, e a manual parou de ser preenchida — o
-- 2T2026 saía vazio no boletim enquanto o dado estava inteiro aqui. Uma
-- fonte só por indicador: a que a migração `0003` mantém.
--
-- Chave da tabela é (ano, trimestre); a edição é montada a partir dela, para
-- não depender de texto livre em coluna de período.

with edicoes as (
    select
        (extract(quarter from t)::int::text || 'T'
         || extract(year from t)::int::text)                as edicao,
        (extract(year from t)::int * 4
         + extract(quarter from t)::int)                    as k
    from generate_series(
        make_date(2025, 1, 1),
        date_trunc('quarter', current_date)::date,
        interval '3 months'
    ) as t
),

cbic as (
    select ano * 4 + trimestre as k, *
    from {{ source('conjuntura_bronze', 'bronze_cbic_lancamentos_vendas') }}
)

select
    e.edicao,
    x.regiao,
    x.total                                                  as "TOTAL",
    x.mcmv                                                   as "MCMV",
    round((x.mcmv::numeric / nullif(x.total, 0) * 100), 0)    as "% MCMV"
from edicoes e
join cbic d on d.k = e.k
cross join lateral (
    select 'NORTE'        as regiao, 1 as ordem,
           d.cbic_lancamentos_regiao_norte              as total,
           d.cbic_lancamentos_mcmv_regiao_norte         as mcmv
    union all select 'NORDESTE',     2,
           d.cbic_lancamentos_regiao_nordeste,      d.cbic_lancamentos_mcmv_regiao_nordeste
    union all select 'CENTRO-OESTE', 3,
           d.cbic_lancamentos_regiao_centro_oeste,  d.cbic_lancamentos_mcmv_regiao_centro_oeste
    union all select 'SUDESTE',      4,
           d.cbic_lancamentos_regiao_sudeste,       d.cbic_lancamentos_mcmv_regiao_sudeste
    union all select 'SUL',          5,
           d.cbic_lancamentos_regiao_sul,           d.cbic_lancamentos_mcmv_regiao_sul
) x
order by e.edicao, x.ordem
