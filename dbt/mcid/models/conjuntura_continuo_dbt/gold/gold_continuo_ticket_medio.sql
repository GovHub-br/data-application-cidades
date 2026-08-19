{{ config(materialized="table") }}

-- Gold do conjuntura contínuo: Ticket Médio de Lançamentos (MRV, Direcional,
-- Tenda, Cury) vs INCC — número índice e variações trimestrais, base 4T2020.
-- Página 6/7 (Preços). Dado MANUAL (boletim.xlsx / manual_conjuntura.dados_trimestrais).
select
    periodo,
    ano,
    trimestre,
    make_date(
        ano::int, (nullif(left(trimestre, 1), '')::int - 1) * 3 + 1, 1
    ) as data_referencia,
    precos_incc_tri,
    incc_var_tri_ant,
    incc_var_acum_4t2020,
    ticket_medio_lancamentos_mrv,
    ticket_medio_lancamentos_mrv_var_tri_ant,
    ticket_medio_lancamentos_mrv_var_acum_4t2020,
    ticket_medio_lancamentos_direcional,
    ticket_medio_lancamentos_direcional_var_tri_ant,
    ticket_medio_lancamentos_direcional_var_acum_4t2020,
    ticket_medio_lancamentos_tenda,
    ticket_medio_lancamentos_tenda_var_tri_ant,
    ticket_medio_lancamentos_tenda_var_acum_4t2020,
    ticket_medio_lancamentos_cury,
    ticket_medio_lancamentos_cury_var_tri_ant,
    ticket_medio_lancamentos_cury_var_acum_4t2020
from {{ ref("silver_continuo_manual_trimestrais") }}
order by ano desc, trimestre desc
