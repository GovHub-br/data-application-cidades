{{ config(materialized='table') }}

SELECT
    tipo,
    data_referencia,
    valor,
    {{ add_metadata_timestamps('silver') }}
FROM {{ ref('bronze_bacen_financiamentos_imobiliarios') }}