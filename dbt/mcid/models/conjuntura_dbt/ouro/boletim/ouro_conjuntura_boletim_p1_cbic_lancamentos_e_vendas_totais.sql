{{ config(materialized='table') }}

-- Boletim de Conjuntura, página 1: CBIC — Lançamentos e Vendas (totais)
-- Seção do impresso: 2. Lançamentos e Vendas
--
-- Uma linha por EDIÇÃO (coluna `edicao`), com as colunas na ordem
-- impressa. O filtro do Superset seleciona a edição; o dashboard não
-- calcula nada.
--
-- Lê a tabela controlada do CBIC (mantida pelo script `0003`), e não a
-- planilha manual, que guardava os mesmos totais em paralelo e parou de ser
-- preenchida. A CBIC revisa trimestres já publicados, então a série corrente
-- muda: o que cada edição mostrou fica nos snapshots.
--
-- Os acumulados de 12 meses são somados aqui a partir dos quatro trimestres
-- que terminam na referência. Antes vinham de colunas próprias na planilha,
-- que precisavam ser digitadas a cada edição e podiam divergir dos totais.

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

s as (
    select
        ano * 4 + trimestre                     as k,
        cbic_lancamentos_total                  as lt,
        cbic_lancamentos_mcmv                   as lm,
        cbic_vendas_total                       as vt,
        cbic_vendas_mcmv                        as vm,
        -- janela de 4 trimestres pela chave linear, não por `rows`: se um
        -- trimestre faltar na tabela, `rows between 3 preceding` somaria o
        -- ano errado silenciosamente. `range` sobre k pula o buraco.
        sum(cbic_lancamentos_total) over j      as lt12,
        sum(cbic_lancamentos_mcmv)  over j      as lm12,
        sum(cbic_vendas_total)      over j      as vt12,
        sum(cbic_vendas_mcmv)       over j      as vm12
    from {{ source('conjuntura', 'bnz_cbic_lancamentos_vendas') }}
    window j as (order by ano * 4 + trimestre range between 3 preceding and current row)
)

select
    e.edicao,
    x.rotulo                as periodo,
    x.lt                    as "Lançamentos TOTAL",
    x.lm                    as "Lançamentos MCMV",
    x.lt - x.lm             as "Lançamentos DEMAIS",
    x.vt                    as "Vendas TOTAL",
    x.vm                    as "Vendas MCMV",
    x.vt - x.vm             as "Vendas DEMAIS"
from edicoes e
cross join lateral (
    select 'Trimestre selecionado' as rotulo, 1 as ordem, lt, lm, vt, vm from s where k = e.k
    union all select 'Trimestre anterior',          2, lt, lm, vt, vm       from s where k = e.k - 1
    union all select 'Mesmo trim. do ano anterior', 3, lt, lm, vt, vm       from s where k = e.k - 4
    union all select '12 meses até a referência',   4, lt12, lm12, vt12, vm12 from s where k = e.k
    union all select '12 meses anteriores',         5, lt12, lm12, vt12, vm12 from s where k = e.k - 4
) x
order by e.edicao, x.ordem
