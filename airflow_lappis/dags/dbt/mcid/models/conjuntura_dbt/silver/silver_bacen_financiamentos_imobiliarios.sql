{{ config(materialized='table') }}

SELECT
    tipo,
    data_referencia,
    valor
FROM {{ ref('bronze_bacen_financiamentos_imobiliarios') }}