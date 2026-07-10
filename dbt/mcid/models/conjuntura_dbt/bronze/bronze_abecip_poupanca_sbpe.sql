{{ config(materialized='table') }}

SELECT
    CAST(data_referencia        AS DATE)      AS data_referencia,
    CAST(deposito               AS NUMERIC)   AS deposito,
    CAST(retirada               AS NUMERIC)   AS retirada,
    CAST(captacao_liquida_valor AS NUMERIC)   AS captacao_liquida_valor,
    CAST(captacao_liquida_pct   AS NUMERIC)   AS captacao_liquida_pct,
    CAST(rendimento             AS NUMERIC)   AS rendimento,
    CAST(saldo                  AS NUMERIC)   AS saldo,
    CAST(fonte                  AS VARCHAR)   AS fonte,
    CAST(dt_ingest              AS TIMESTAMP) AS dt_ingest
FROM {{ source('abecip', 'poupanca_sbpe_mensal') }}