{{ config(materialized='table') }}

SELECT
    variavel_id,
    periodo,
    CAST(valor AS NUMERIC) / 10.0 AS valor_percentual
FROM {{ ref('bronze_ibge_pib_construcao_civil') }}
WHERE variavel_id IN (6562, 6563, 6564)