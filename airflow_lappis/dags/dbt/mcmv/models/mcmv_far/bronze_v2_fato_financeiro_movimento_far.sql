{{ config(materialized='table') }}

WITH source AS (
    SELECT *
    FROM {{ source('__dados_brutos', 'novo_mcmv_far_financeiro_mensal') }}
)

SELECT
    nu_apf,
    dt_movimento,
    dt_remessa,
    dt_liberacao_recurso,
    dt_evento,
    co_tipo_movimento,
    co_tipo_lib_recurso,
    ic_credito,
    {{ parse_financial_value('vr_movimento') }} AS vr_movimento,
    no_identificador,
    {{ parse_financial_value('vr_pago_obra_empreendimento') }} AS vr_pago_obra_empreendimento,
    {{ parse_financial_value('vr_pago_terreno') }} AS vr_pago_terreno,
    {{ parse_financial_value('vr_pago_pts') }} AS vr_pago_pts,
    {{ parse_financial_value('vr_pago_equipamentos_publicos') }} AS vr_pago_equipamentos_publicos,
    {{ parse_financial_value('vr_pago_aporte_suplementacao') }} AS vr_pago_aporte_suplementacao,
    {{ parse_financial_value('vr_pago_despesas_manutencao') }} AS vr_pago_despesas_manutencao,
    {{ parse_financial_value('vr_pago_despesas_incc') }} AS vr_pago_despesas_incc,
    {{ parse_financial_value('vr_pago_cartorios_legalizacao') }} AS vr_pago_cartorios_legalizacao,
    {{ parse_financial_value('vr_liberado') }} AS vr_liberado,
    dh_gravacao,
    arquivo_de_origem,
    criado_em
FROM source
