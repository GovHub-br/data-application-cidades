{{ config(materialized='table') }}

WITH base_consolidada AS (
    SELECT *
    FROM {{ ref('bronze_v2_base_consolidada_empreendimento_mensal') }}
),
dim_meta_uf AS (
    SELECT *
    FROM {{ ref('bronze_v2_dim_meta_uf_far') }}
)

SELECT
    b.no_uf,
    b.dt_movimento,
    
    -- Indicadores
    COUNT(DISTINCT b.nu_apf) AS qt_empreendimentos,
    COUNT(DISTINCT b.co_municipio_ibge) AS qt_municipios_atendidos,
    SUM(b.nu_qt_uh_empreendimento) AS qt_uh_contratadas,
    SUM(b.qt_uh_concluidas) AS qt_uh_concluidas,
    SUM(b.qt_uh_alienada) AS qt_uh_alienadas,
    SUM(b.vr_empreendimento_far) AS vr_total_contratado,
    SUM(b.vr_liberado_acumulado) AS vr_total_liberado,
    AVG(b.pc_obra_realizada) AS pc_execucao_fisica_media,
    
    -- Financeira percentual
    CASE WHEN SUM(b.vr_empreendimento_far) > 0 THEN SUM(b.vr_liberado_acumulado) / SUM(b.vr_empreendimento_far) ELSE 0 END AS pc_execucao_financeira,
    
    -- Alertas agregados
    SUM(b.flag_atraso_fisico + b.flag_paralisada + b.flag_invadida + b.flag_sem_habitese + b.flag_baixa_execucao_financeira + b.flag_inconsistencia_uh) AS qt_alertas_criticos,
    
    -- Metas
    CASE WHEN SUM(b.nu_qt_uh_empreendimento) >= COALESCE(MAX(m.meta_uh), 0) THEN 1 ELSE 0 END AS ic_uf_acima_meta
    
FROM base_consolidada b
LEFT JOIN dim_meta_uf m ON b.no_uf = m.no_uf AND EXTRACT(YEAR FROM CAST(b.dt_movimento AS DATE)) = m.ano
GROUP BY
    b.no_uf,
    b.dt_movimento
