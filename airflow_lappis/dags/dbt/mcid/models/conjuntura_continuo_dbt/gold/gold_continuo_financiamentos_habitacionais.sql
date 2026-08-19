{{ config(materialized='table') }}

-- Gold do conjuntura contínuo: Financiamentos Habitacionais (UH) — FGTS-PJ e
-- SBPE Construção, trimestral + acumulado 12 meses. Página 2, seção 3.
-- Dado MANUAL (boletim.xlsx / manual_conjuntura.dados_trimestrais).

select
    periodo,
    ano,
    trimestre,
    make_date(ano::int, (nullif(left(trimestre, 1), '')::int - 1) * 3 + 1, 1) as data_referencia,
    financ_hab_fgts_pj,
    financ_hab_fgts_pj_acumulado_12_meses,
    financ_hab_sbpe_constr,
    financ_hab_sbpe_constr_acumulado_12_meses
from {{ ref('silver_continuo_manual_trimestrais') }}
order by ano desc, trimestre desc
