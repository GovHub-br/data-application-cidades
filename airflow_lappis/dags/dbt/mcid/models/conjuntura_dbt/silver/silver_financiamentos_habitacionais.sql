{{ config(materialized='table') }}

WITH fgts AS (

    SELECT
        ano,
        trimestre,

        SUM(financiamento_pf_uh_total_geral)
            AS fgts_uh

    FROM {{ source(
        'conjuntura_bronze',
        'bronze_fgts_financiamentos_habitacionais'
    ) }}

    GROUP BY ano, trimestre

),

abecip AS (

    SELECT
        ano,
        trimestre,

        SUM(sbpe_const)
            AS sbpe_uh

    FROM {{ source(
        'conjuntura_bronze',
        'bronze_abecip_sbpe_financiamentos_habitacionais'
    ) }}

    GROUP BY ano, trimestre

)

SELECT
    fgts.ano,
    fgts.trimestre,

    fgts.fgts_uh,
    abecip.sbpe_uh

FROM fgts

LEFT JOIN abecip
    ON fgts.ano = abecip.ano
   AND fgts.trimestre = abecip.trimestre