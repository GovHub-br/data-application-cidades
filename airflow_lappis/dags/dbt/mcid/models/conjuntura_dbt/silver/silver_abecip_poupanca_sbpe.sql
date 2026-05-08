{{ config(materialized='table') }}

SELECT
    data_referencia,
    deposito,
    retirada,
    captacao_liquida_valor,
    captacao_liquida_pct,
    rendimento,
    saldo,
    -- Captação líquida em bilhões
    ROUND(captacao_liquida_valor::numeric / 1e3, 1) AS captacao_liquida_bi,
    -- Acumulado 12 meses
    SUM(captacao_liquida_valor) OVER (
        ORDER BY data_referencia
        ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
    )                                               AS captacao_acum_12m,
    fonte,
    dt_ingest
FROM {{ ref('bronze_abecip_poupanca_sbpe') }}