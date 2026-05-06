{{ config(materialized='table') }}

WITH base AS (
    SELECT ano, mes, banco, valor_bi, uh
    FROM {{ ref('silver_abecip_novos_financiamentos_imobiliarios') }}
),

total AS (
    SELECT ano, mes, SUM(valor_bi) AS total_bi, SUM(uh) AS total_uh
    FROM base WHERE banco != 'TOTAL'
    GROUP BY ano, mes
),

com_perc AS (
    SELECT
        b.ano, b.mes, b.banco,
        ROUND(b.valor_bi::numeric, 1)                                       AS valor_bi,
        ROUND((b.valor_bi / NULLIF(t.total_bi, 0)) * 100, 1)               AS perc_valor,
        b.uh,
        ROUND((b.uh::numeric / NULLIF(t.total_uh, 0)) * 100, 1)           AS perc_uh,
        CASE b.banco
            WHEN 'TOTAL'                THEN 0
            WHEN 'CEF (CAIXA)'          THEN 1
            WHEN 'ITAU'                 THEN 2
            WHEN 'BRADESCO'             THEN 3
            WHEN 'SANTANDER'            THEN 4
            WHEN 'BRB'                  THEN 5
            WHEN 'BB (BANCO DO BRASIL)' THEN 6
            WHEN 'DEMAIS'               THEN 7
        END AS ordem
    FROM base b
    LEFT JOIN total t ON b.ano = t.ano AND b.mes = t.mes
),

ano_anterior AS (
    SELECT ano, mes, banco, valor_bi AS valor_bi_ant, uh AS uh_ant
    FROM com_perc
)

SELECT
    c.ano, c.mes, c.banco,
    c.valor_bi,
    c.perc_valor                                                            AS perc_valor,
    ROUND(((c.valor_bi / NULLIF(a.valor_bi_ant, 0)) - 1) * 100, 0)        AS var_ano_valor,
    c.uh,
    c.perc_uh,
    ROUND(((c.uh::numeric / NULLIF(a.uh_ant, 0)) - 1) * 100, 0)           AS var_ano_uh,
    c.ordem
FROM com_perc c
LEFT JOIN ano_anterior a ON c.banco = a.banco
    AND a.ano = c.ano - 1
    AND a.mes = c.mes
ORDER BY c.ano, c.mes, c.ordem