{{ config(materialized='table') }}

SELECT
    data_referencia,
    MAX(CASE WHEN variavel_id = 48   THEN valor / 100.0 END) AS custo_m2,
    MAX(CASE WHEN variavel_id = 1196 THEN valor / 100.0 END) AS var_mes,
    MAX(CASE WHEN variavel_id = 1197 THEN valor / 100.0 END) AS var_ano,
    MAX(CASE WHEN variavel_id = 1198 THEN valor / 100.0 END) AS var_12m
FROM {{ source('conjuntura_bronze', 'bronze_ibge_sinapi') }}
GROUP BY data_referencia