{{ config(materialized='table') }}

WITH base AS (
    SELECT
        ano,
        mes,
        id_agente,
        agente,
        posicao_mes_rs_milhoes,
        posicao_mes_unidades,
        posicao_ano_rs_milhoes,
        posicao_ano_unidades
    FROM {{ source('conjuntura_bronze', 'bronze_abecip_novos_financiamentos_imobiliarios') }}
),

agrupado AS (
    SELECT
        ano,
        mes,
        CASE
            WHEN agente = 'CAIXA'           THEN 'CEF (CAIXA)'
            WHEN agente = 'ITAÚ UNIBANCO'   THEN 'ITAU'
            WHEN agente = 'BRADESCO'        THEN 'BRADESCO'
            WHEN agente = 'SANTANDER'       THEN 'SANTANDER'
            WHEN agente = 'BRB'             THEN 'BRB'
            WHEN agente = 'BANCO DO BRASIL' THEN 'BB (BANCO DO BRASIL)'
            ELSE 'DEMAIS'
        END                                             AS banco,
        SUM(posicao_ano_rs_milhoes)                     AS valor_ano_milhoes,
        SUM(posicao_ano_unidades)                       AS uh_ano
    FROM base
    GROUP BY ano, mes, banco
),

total AS (
    SELECT
        ano,
        mes,
        'TOTAL'                                         AS banco,
        SUM(posicao_ano_rs_milhoes)                     AS valor_ano_milhoes,
        SUM(posicao_ano_unidades)                       AS uh_ano
    FROM base
    GROUP BY ano, mes
)

SELECT ano, mes, banco,
    (valor_ano_milhoes / 1000.0)::numeric               AS valor_bi,
    uh_ano                                              AS uh
FROM agrupado

UNION ALL

SELECT ano, mes, banco,
    (valor_ano_milhoes / 1000.0)::numeric,
    uh_ano
FROM total