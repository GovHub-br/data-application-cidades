{{ config(materialized='table') }}

WITH periodos AS (
    SELECT
        'DEZ 25 vs. NOV 25'  AS periodo, 1 AS ordem,
        '2025-12-01'::date   AS data_atual,
        '2025-11-01'::date   AS data_anterior,
        '2024-12-01'::date   AS data_ano_anterior
    UNION ALL SELECT 'DEZ 25 vs. DEZ 24', 2, '2025-12-01', '2024-12-01', '2023-12-01'
    UNION ALL SELECT 'JAN-DEZ/25',        3, '2025-12-01', '2024-12-01', '2023-12-01'
    UNION ALL SELECT 'JAN-DEZ/24',        4, '2024-12-01', '2023-12-01', '2022-12-01'
),

-- IMOB
imob AS (
    SELECT data_referencia, close_fim_mes, var_mes, var_12_meses
    FROM {{ ref('silver_imob_infomoney') }}
),

-- FIPEZAP
fipezap AS (
    SELECT data_referencia, var_mensal, var_ano
    FROM {{ ref('silver_fipezap_locacao') }}
),

-- ICST
icst AS (
    SELECT data_referencia, icst_sem_ajuste_sazonal, var_mes, var_12_meses
    FROM {{ ref('silver_fgv_icst') }}
),

-- ABRAMAT
abramat AS (
    SELECT data_referencia, var_mes, var_12_meses
    FROM {{ ref('silver_abramat_indice') }}
)

SELECT
    p.periodo                                                               AS "periodo",

    -- IMOB
    ROUND(((ia.close_fim_mes / NULLIF(ib.close_fim_mes, 0)) - 1) * 100, 1) AS imob_var,

    -- FIPEZAP
    CASE
        WHEN p.ordem IN (1) THEN ROUND(fa.var_mensal * 100, 2)
        WHEN p.ordem IN (2) THEN ROUND(((1 + fa.var_mensal) / NULLIF(1 + fb.var_mensal, 0) - 1) * 100, 2)
        WHEN p.ordem IN (3) THEN ROUND(fa.var_ano * 100, 2)
        WHEN p.ordem IN (4) THEN ROUND(fb.var_ano * 100, 2)
    END                                                                     AS fipezap_var,

    -- ICST
    CASE
        WHEN p.ordem IN (1) THEN ROUND(((ca.icst_sem_ajuste_sazonal / NULLIF(cb.icst_sem_ajuste_sazonal, 0)) - 1) * 100, 2)
        WHEN p.ordem IN (2) THEN ROUND(((ca.icst_sem_ajuste_sazonal / NULLIF(cc.icst_sem_ajuste_sazonal, 0)) - 1) * 100, 2)
        WHEN p.ordem IN (3) THEN ROUND(((ca.icst_sem_ajuste_sazonal / NULLIF(cc.icst_sem_ajuste_sazonal, 0)) - 1) * 100, 2)
        WHEN p.ordem IN (4) THEN ROUND(((cc.icst_sem_ajuste_sazonal / NULLIF(cd.icst_sem_ajuste_sazonal, 0)) - 1) * 100, 2)
    END                                                                     AS icst_var,

    -- ABRAMAT
    CASE
        WHEN p.ordem = 1 THEN aa.var_mes
        WHEN p.ordem = 2 THEN aa.var_12_meses
        WHEN p.ordem = 3 THEN aa.var_12_meses
        WHEN p.ordem = 4 THEN ab.var_12_meses
    END                                                                     AS abramat_var

FROM periodos p

-- IMOB joins
LEFT JOIN imob ia ON ia.data_referencia = p.data_atual
LEFT JOIN imob ib ON ib.data_referencia = p.data_anterior

-- FIPEZAP joins
LEFT JOIN fipezap fa ON fa.data_referencia = p.data_atual
LEFT JOIN fipezap fb ON fb.data_referencia = p.data_anterior

-- ICST joins
LEFT JOIN icst ca ON ca.data_referencia = p.data_atual
LEFT JOIN icst cb ON cb.data_referencia = p.data_anterior
LEFT JOIN icst cc ON cc.data_referencia = p.data_ano_anterior
LEFT JOIN icst cd ON cd.data_referencia = '2023-12-01'

-- ABRAMAT joins
LEFT JOIN abramat aa ON aa.data_referencia = p.data_atual
LEFT JOIN abramat ab ON ab.data_referencia = p.data_anterior

ORDER BY p.ordem