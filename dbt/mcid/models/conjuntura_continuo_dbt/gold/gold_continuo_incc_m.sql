{{ config(materialized="table") }}

-- Gold do conjuntura contínuo: INCC-M — número índice e variações mensais.
-- Página 6 (Preços). Fonte: FGV (automatizado).
select mes, indice, var_mes, var_ano, var_12_meses
from {{ ref("silver_continuo_fgv_incc_m") }}
order by mes desc
