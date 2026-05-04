{{ config(materialized='table') }}

WITH source AS (
    SELECT *
    FROM {{ ref('bronze_v2_fato_financeiro_movimento_far') }}
)

SELECT
    nu_apf,
    dt_movimento,
    SUM(vr_movimento) AS vr_movimentado_mes,
    SUM(vr_liberado) AS vr_liberado_acumulado,
    SUM(vr_pago_obra_empreendimento) AS vr_pago_obra_acumulado,
    SUM(vr_pago_terreno) AS vr_pago_terreno_acumulado,
    SUM(vr_pago_pts) AS vr_pago_pts_acumulado,
    SUM(vr_pago_equipamentos_publicos) AS vr_pago_equipamentos_publicos_acumulado,
    SUM(vr_pago_despesas_manutencao) AS vr_pago_manutencao_acumulado,
    SUM(vr_pago_despesas_incc) AS vr_pago_incc_acumulado,
    SUM(vr_pago_cartorios_legalizacao) AS vr_pago_cartorios_legalizacao_acumulado,
    COUNT(*) AS qt_movimentos_mes,
    MAX(dt_liberacao_recurso) AS dt_ultima_liberacao_recurso
FROM source
GROUP BY
    nu_apf,
    dt_movimento
