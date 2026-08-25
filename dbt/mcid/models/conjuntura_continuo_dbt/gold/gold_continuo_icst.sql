{{ config(materialized='table') }}

-- Gold do conjuntura contínuo: Índice ICST (confiança na construção, FGV-IBRE)
-- — série original (sem ajuste sazonal) e variações mensais. Página 7
-- (seção 8). Fonte: FGV-IBRE (automatizado) — substitui o dado MANUAL
-- (boletim.xlsx) usado até 2026-08-25. A fonte não publica variações
-- prontas, então calculadas aqui via window function sobre o índice.

select
    periodo,
    extract(year from data_referencia)::int  as ano,
    extract(month from data_referencia)::int as mes,
    data_referencia,
    icst_sem_ajuste_sazonal,
    (icst_sem_ajuste_sazonal - lag(icst_sem_ajuste_sazonal, 1) over (order by data_referencia))
        / nullif(lag(icst_sem_ajuste_sazonal, 1) over (order by data_referencia), 0)
        as indice_icst_var_mes_serie_original,
    (icst_sem_ajuste_sazonal - lag(icst_sem_ajuste_sazonal, 12) over (order by data_referencia))
        / nullif(lag(icst_sem_ajuste_sazonal, 12) over (order by data_referencia), 0)
        as indice_icst_var_mes_vs_mes_ano_ant_serie_original,
    (icst_sem_ajuste_sazonal - lag(icst_sem_ajuste_sazonal, extract(month from data_referencia)::int)
        over (order by data_referencia))
        / nullif(lag(icst_sem_ajuste_sazonal, extract(month from data_referencia)::int)
            over (order by data_referencia), 0)
        as indice_icst_var_acum_ano_serie_original
from {{ ref('silver_continuo_fgv_icst') }}
order by data_referencia desc
