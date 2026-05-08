{{ config(materialized='table') }}

SELECT
    data_referencia,
    icst_com_ajuste_sazonal,
    icst_sem_ajuste_sazonal,
    ROUND(
        ((icst_sem_ajuste_sazonal / NULLIF(LAG(icst_sem_ajuste_sazonal, 1) OVER (ORDER BY data_referencia), 0)) - 1) * 100,
        2
    ) AS var_mes,
    ROUND(
        ((icst_sem_ajuste_sazonal / NULLIF(LAG(icst_sem_ajuste_sazonal, 12) OVER (ORDER BY data_referencia), 0)) - 1) * 100,
        2
    ) AS var_12_meses,
    dt_ingest
FROM {{ source('conjuntura_bronze', 'bronze_fgv_icst') }}