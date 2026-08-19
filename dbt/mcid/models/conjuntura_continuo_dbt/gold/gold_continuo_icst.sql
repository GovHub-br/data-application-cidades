{{ config(materialized="table") }}

-- Gold do conjuntura contínuo: Índice ICST (confiança na construção, FGV-IBRE)
-- — série original (sem ajuste sazonal) e variações mensais. Página 7
-- (seção 8). Dado MANUAL (boletim.xlsx / manual_conjuntura.dados_mensais).
-- Obs.: silver_continuo_fgv_icst (automatizado, com e sem ajuste sazonal)
-- fica como apoio.
select
    periodo,
    ano,
    mes,
    make_date(ano::int, mes::int, 1) as data_referencia,
    icst_serie_original_sem_ajuste_sazonal,
    indice_icst_var_mes_serie_original,
    indice_icst_var_mes_vs_mes_ano_ant_serie_original,
    indice_icst_var_acum_ano_serie_original
from {{ ref("silver_continuo_manual_mensais") }}
where icst_serie_original_sem_ajuste_sazonal is not null
order by ano desc, mes desc
