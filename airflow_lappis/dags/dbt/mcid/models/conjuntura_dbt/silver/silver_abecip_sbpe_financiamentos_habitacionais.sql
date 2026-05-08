{{ config(materialized='table') }}

SELECT
    trimestre,
    ano,
    mes,
    -- UH
    sbpe_const,
    sbpe_aquisicao,
    sbpe_total,
    -- Valores em R$ milhões
    sbpe_const_milhoes,
    sbpe_aquisicao_milhoes,
    sbpe_total_milhoes,
    -- Aquisição por condição de uso
    sbpe_aq_novos_uh,
    sbpe_aq_usados_uh,
    sbpe_aq_novos_milhoes,
    sbpe_aq_usados_milhoes,
    -- Calculados
    ROUND(
        ((sbpe_const::numeric / NULLIF(LAG(sbpe_const) OVER (ORDER BY ano, mes), 0)) - 1) * 100, 1
    ) AS sbpe_const_var_mes,
    ROUND(
        ((sbpe_aquisicao::numeric / NULLIF(LAG(sbpe_aquisicao) OVER (ORDER BY ano, mes), 0)) - 1) * 100, 1
    ) AS sbpe_aq_var_mes,
    ROUND(
        ((sbpe_total::numeric / NULLIF(LAG(sbpe_total) OVER (ORDER BY ano, mes), 0)) - 1) * 100, 1
    ) AS sbpe_total_var_mes,
    ROUND(
        ((sbpe_const::numeric / NULLIF(LAG(sbpe_const, 12) OVER (ORDER BY ano, mes), 0)) - 1) * 100, 1
    ) AS sbpe_const_var_12m,
    ROUND(
        ((sbpe_aquisicao::numeric / NULLIF(LAG(sbpe_aquisicao, 12) OVER (ORDER BY ano, mes), 0)) - 1) * 100, 1
    ) AS sbpe_aq_var_12m,
    ROUND(
        ((sbpe_total::numeric / NULLIF(LAG(sbpe_total, 12) OVER (ORDER BY ano, mes), 0)) - 1) * 100, 1
    ) AS sbpe_total_var_12m
FROM {{ source('conjuntura_bronze', 'bronze_abecip_sbpe_financiamentos_habitacionais') }}