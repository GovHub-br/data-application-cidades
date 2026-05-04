{{ config(materialized='table') }}

WITH base_consolidada AS (
    SELECT *
    FROM {{ ref('bronze_v2_base_consolidada_empreendimento_mensal') }}
)

SELECT
    co_municipio_ibge,
    no_municipio,
    no_uf,
    no_regiao,
    dt_movimento,
    
    -- Indicadores
    COUNT(DISTINCT nu_apf) AS qt_empreendimentos,
    SUM(nu_qt_uh_empreendimento) AS qt_uh_contratadas,
    SUM(qt_uh_concluidas) AS qt_uh_concluidas,
    SUM(qt_uh_alienada) AS qt_uh_alienadas,
    SUM(qt_uh_sem_habitese) AS qt_uh_sem_habitese,
    SUM(vr_empreendimento_far) AS vr_empreendimento_far,
    SUM(vr_liberado_acumulado) AS vr_liberado_acumulado,
    AVG(pc_obra_realizada) AS pc_medio_obra_realizada,
    
    -- Alertas agregados
    SUM(flag_atraso_fisico) AS qt_obras_atrasadas,
    SUM(flag_paralisada) AS qt_obras_paralisadas,
    SUM(flag_invadida) AS qt_obras_invadidas
FROM base_consolidada
GROUP BY
    co_municipio_ibge,
    no_municipio,
    no_uf,
    no_regiao,
    dt_movimento
