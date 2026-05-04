{{ config(materialized='table') }}

WITH dim_empreendimento AS (
    SELECT * FROM {{ ref('bronze_v2_dim_empreendimento_far') }}
),
fato_contratacao AS (
    SELECT * FROM {{ ref('bronze_v2_fato_contratacao_far') }}
),
fato_obra AS (
    SELECT * FROM {{ ref('bronze_v2_fato_obra_mensal_far') }}
),
fato_financeiro AS (
    SELECT * FROM {{ ref('bronze_v2_fato_financeiro_mensal_far') }}
)

SELECT
    -- Identificação
    dim.nu_apf,
    dim.no_nome_empreendimento,
    dim.no_municipio,
    dim.no_uf,
    dim.no_regiao,
    dim.co_municipio_ibge,
    
    -- Contratação
    cont.dt_protocolo,
    cont.no_agente_financeiro,
    cont.vr_empreendimento_far,
    cont.vr_total_contrapartidas,
    dim.nu_qt_uh_empreendimento,
    
    -- Obra
    obra.co_situacao_obra,
    obra.pc_obra_prevista,
    obra.pc_obra_realizada,
    obra.dt_previsao_entrega_do_empreendimento,
    obra.dt_entrega_do_empreendimento,
    
    -- UH
    obra.qt_uh_concluidas,
    obra.qt_uh_alienada,
    obra.nu_qt_uh_a_alienar,
    obra.qt_uh_sem_habitese,
    obra.qt_uh_em_construcao_parcial,
    obra.qt_unidades_habitacionais_invadidas,
    
    -- Financeiro
    fin.vr_liberado_acumulado,
    fin.vr_pago_obra_acumulado,
    fin.vr_movimentado_mes,
    
    -- Alertas
    CASE WHEN obra.pc_obra_realizada < obra.pc_obra_prevista THEN 1 ELSE 0 END AS flag_atraso_fisico,
    CASE WHEN obra.co_classificacao_paralisados IS NOT NULL THEN 1 ELSE 0 END AS flag_paralisada,
    CASE WHEN UPPER(CAST(obra.ic_invadido AS VARCHAR)) IN ('TRUE', 'SIM', '1', 'S', 'T') THEN 1 ELSE 0 END AS flag_invadida,
    CASE WHEN obra.qt_uh_sem_habitese > 0 THEN 1 ELSE 0 END AS flag_sem_habitese,
    CASE WHEN fin.vr_movimentado_mes = 0 AND obra.pc_obra_realizada > 0 THEN 1 ELSE 0 END AS flag_baixa_execucao_financeira,
    CASE WHEN (COALESCE(obra.qt_uh_concluidas, 0) + COALESCE(obra.qt_uh_em_construcao_parcial, 0)) > dim.nu_qt_uh_empreendimento THEN 1 ELSE 0 END AS flag_inconsistencia_uh,
    
    -- Ciclo
    COALESCE(obra.dt_movimento, fin.dt_movimento, cont.dt_movimento) AS dt_movimento
FROM dim_empreendimento dim
LEFT JOIN fato_contratacao cont ON dim.nu_apf = cont.nu_apf
LEFT JOIN fato_obra obra ON dim.nu_apf = obra.nu_apf
LEFT JOIN fato_financeiro fin ON dim.nu_apf = fin.nu_apf AND obra.dt_movimento = fin.dt_movimento
