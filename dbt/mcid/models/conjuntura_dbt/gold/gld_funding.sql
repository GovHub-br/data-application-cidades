{{ config(materialized='table') }}

-- Gold do conjuntura contínuo: Estrutura de Funding — SBPE, FGTS, LCI, LCA,
-- CRI, CRA, LIG (estoques, R$ bi). Página 4. Dado MANUAL (boletim.xlsx /
-- conjuntura.bnz_manual_dados_mensais).

select
    periodo,
    ano,
    mes,
    make_date(ano::int, mes::int, 1) as data_referencia,
    funding_sbpe,
    funding_fgts,
    anbima_estoque_lci,
    anbima_estoque_lca,
    anbima_estoque_cri,
    anbima_estoque_cra,
    anbima_estoque_lig
from {{ ref('slv_manual_mensais') }}
where coalesce(funding_sbpe, funding_fgts) is not null
order by ano desc, mes desc
