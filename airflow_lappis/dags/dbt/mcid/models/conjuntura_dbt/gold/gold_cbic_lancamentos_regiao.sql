{{ config(materialized='table') }}

WITH periodos AS (

    SELECT
        ano,
        trimestre,

        cbic_lancamentos_regiao_norte              AS total_norte,
        cbic_lancamentos_mcmv_regiao_norte         AS mcmv_norte,
        cbic_lancamentos_mcmv_perc_regiao_norte    AS perc_norte,

        cbic_lancamentos_regiao_nordeste              AS total_nordeste,
        cbic_lancamentos_mcmv_regiao_nordeste         AS mcmv_nordeste,
        cbic_lancamentos_mcmv_perc_regiao_nordeste    AS perc_nordeste,

        cbic_lancamentos_regiao_centro_oeste              AS total_centro_oeste,
        cbic_lancamentos_mcmv_regiao_centro_oeste         AS mcmv_centro_oeste,
        cbic_lancamentos_mcmv_perc_regiao_centro_oeste    AS perc_centro_oeste,

        cbic_lancamentos_regiao_sudeste              AS total_sudeste,
        cbic_lancamentos_mcmv_regiao_sudeste         AS mcmv_sudeste,
        cbic_lancamentos_mcmv_perc_regiao_sudeste    AS perc_sudeste,

        cbic_lancamentos_regiao_sul              AS total_sul,
        cbic_lancamentos_mcmv_regiao_sul         AS mcmv_sul,
        cbic_lancamentos_mcmv_perc_regiao_sul    AS perc_sul

    FROM {{ ref('silver_cbic_lancamentos_vendas') }}

),

regioes AS (

    SELECT
        'NORTE' AS regiao,
        total_norte AS total,
        mcmv_norte AS mcmv,
        perc_norte AS perc_mcmv,
        ano,
        trimestre
    FROM periodos

    UNION ALL

    SELECT
        'NORDESTE',
        total_nordeste,
        mcmv_nordeste,
        perc_nordeste,
        ano,
        trimestre
    FROM periodos

    UNION ALL

    SELECT
        'CENTRO-OESTE',
        total_centro_oeste,
        mcmv_centro_oeste,
        perc_centro_oeste,
        ano,
        trimestre
    FROM periodos

    UNION ALL

    SELECT
        'SUDESTE',
        total_sudeste,
        mcmv_sudeste,
        perc_sudeste,
        ano,
        trimestre
    FROM periodos

    UNION ALL

    SELECT
        'SUL',
        total_sul,
        mcmv_sul,
        perc_sul,
        ano,
        trimestre
    FROM periodos

),

periodo_atual AS (

    SELECT *
    FROM regioes
    WHERE ano = 2025
      AND trimestre = 4

),

periodo_anterior AS (

    SELECT *
    FROM regioes
    WHERE ano = 2025
      AND trimestre = 3

),

periodo_ano_anterior AS (

    SELECT *
    FROM regioes
    WHERE ano = 2024
      AND trimestre = 4

)

SELECT
    atual.regiao,
    atual.total,
    atual.mcmv,

    ROUND(atual.perc_mcmv::numeric, 1) AS perc_mcmv_4t25,
    ROUND(ant.perc_mcmv::numeric, 1)   AS perc_mcmv_3t25,
    ROUND(aa.perc_mcmv::numeric, 1)    AS perc_mcmv_4t24

FROM periodo_atual atual

LEFT JOIN periodo_anterior ant
    ON atual.regiao = ant.regiao

LEFT JOIN periodo_ano_anterior aa
    ON atual.regiao = aa.regiao

ORDER BY
    CASE atual.regiao
        WHEN 'NORTE' THEN 1
        WHEN 'NORDESTE' THEN 2
        WHEN 'CENTRO-OESTE' THEN 3
        WHEN 'SUDESTE' THEN 4
        WHEN 'SUL' THEN 5
    END