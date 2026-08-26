{{ config(materialized="table") }}

select
    data_referencia,
    icst_com_ajuste_sazonal,
    icst_sem_ajuste_sazonal,
    round(
        (
            (
                icst_sem_ajuste_sazonal / nullif(
                    lag(icst_sem_ajuste_sazonal, 1) over (order by data_referencia), 0
                )
            )
            - 1
        )
        * 100,
        2
    ) as var_mes,
    round(
        (
            (
                icst_sem_ajuste_sazonal / nullif(
                    lag(icst_sem_ajuste_sazonal, 12) over (order by data_referencia), 0
                )
            )
            - 1
        )
        * 100,
        2
    ) as var_12_meses,
    {{ add_metadata_timestamps("silver") }}
from {{ source("conjuntura_bronze", "bronze_fgv_icst") }}
