{{ config(materialized='table') }}

WITH base AS (
    SELECT
        ano,
        mes,
        trimestre,
        saldo,
        estoque,
        saldo_var_mes,
        estoque_var_ano
    FROM {{ ref('silver_novo_caged') }}
),

-- Agregação trimestral: saldo acumulado nos 3 meses, estoque do último mês
trimestral AS (
    SELECT
        ano,
        trimestre,
        MIN(mes)            AS mes_ini,
        MAX(mes)            AS mes_fim,
        SUM(saldo)          AS saldo_tri,
        MAX(estoque)        AS estoque_fim_tri
    FROM base
    GROUP BY ano, trimestre
),

-- Variações trimestrais
com_var AS (
    SELECT
        t.ano,
        t.trimestre,
        t.mes_ini,
        t.mes_fim,
        t.saldo_tri,
        t.estoque_fim_tri,
        -- Var saldo vs trimestre anterior
        ROUND(
            ((t.saldo_tri::numeric / NULLIF(LAG(t.saldo_tri) OVER (ORDER BY t.ano, t.trimestre), 0)) - 1) * 100, 0
        ) AS saldo_var_tri,
        -- Var estoque vs mesmo trimestre ano anterior
        ROUND(
            ((t.estoque_fim_tri::numeric / NULLIF(LAG(t.estoque_fim_tri, 4) OVER (ORDER BY t.ano, t.trimestre), 0)) - 1) * 100, 0
        ) AS estoque_var_ano
    FROM trimestral t
),

-- Acumulado anual (JAN-MAR = 1T, JAN-JUN = 2T, etc)
acumulado AS (
    SELECT
        ano,
        trimestre,
        SUM(saldo_tri) OVER (PARTITION BY ano ORDER BY trimestre) AS saldo_acum_ano,
        MAX(estoque_fim_tri) OVER (PARTITION BY ano ORDER BY trimestre) AS estoque_acum_ano
    FROM com_var
)

SELECT
    v.ano,
    v.trimestre,
    v.mes_ini,
    v.mes_fim,
    v.saldo_tri                             AS criacao_liquida_saldo,
    v.saldo_var_tri                         AS saldo_var_tri_pct,
    v.estoque_fim_tri                       AS total_postos_estoque,
    v.estoque_var_ano                       AS estoque_var_ano_pct,
    a.saldo_acum_ano                        AS saldo_acum_ano,
    a.estoque_acum_ano                      AS estoque_acum_ano
FROM com_var v
JOIN acumulado a ON v.ano = a.ano AND v.trimestre = a.trimestre
ORDER BY v.ano, v.trimestre