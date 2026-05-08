{{ config(materialized='table') }}

SELECT
    nome_empresa,
    ano_balanco,
    trimestre_balanco,

    lancamento,
    vendas

FROM {{ source('conjuntura_bronze', 'bronze_balancos_empresas') }}