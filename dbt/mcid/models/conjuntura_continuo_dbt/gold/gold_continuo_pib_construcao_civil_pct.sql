{{ config(materialized='table') }}

-- Gold do conjuntura contínuo: PIB da construção civil em % de crescimento,
-- no formato do boletim (página 1) — últimos trimestres do exercício.
-- Origem: silver_continuo_pib_construcao_civil_pct (dado MANUAL, dessazonalizado).

with base as (
    select * from {{ ref('silver_continuo_pib_construcao_civil_pct') }}
)

select
    periodo,
    ano,
    trimestre,
    make_date(ano, (trimestre - 1) * 3 + 1, 1) as data_referencia,
    pib_const_trimestre_anterior      as var_trim_trim_anterior,
    pib_const_taxa_acumulada_ano      as var_acumulada_ano,
    pib_const_taxa_acumulada_4_trimestres as var_acumulada_4_trimestres
from base
order by ano desc, trimestre desc
