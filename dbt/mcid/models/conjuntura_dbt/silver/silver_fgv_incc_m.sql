{{ config(materialized="table") }}

select
    mes,
    data_referencia,
    indice,
    var_ano,
    var_mes,
    var_12_meses,
    -- Variação trimestral (índice fim tri / índice 3 meses antes)
    round(
        ((indice / nullif(lag(indice, 3) over (order by data_referencia), 0)) - 1) * 100,
        1
    ) as var_tri,
    -- Trimestre
    case
        when extract(month from data_referencia) in (1, 2, 3)
        then 1
        when extract(month from data_referencia) in (4, 5, 6)
        then 2
        when extract(month from data_referencia) in (7, 8, 9)
        then 3
        when extract(month from data_referencia) in (10, 11, 12)
        then 4
    end as trimestre,
    extract(year from data_referencia)::int as ano,
    {{ add_metadata_timestamps("silver") }}
from {{ source("conjuntura_bronze", "bronze_fgv_incc_m") }}
