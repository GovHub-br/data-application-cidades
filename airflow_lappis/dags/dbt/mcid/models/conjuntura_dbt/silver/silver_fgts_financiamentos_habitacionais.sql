{{ config(materialized='table') }}

SELECT
    ano,
    mes,
    trimestre,
    financiamento_pf_uh_total_geral,
    financiamento_pf_valor_total_geral,
    financiamento_pf_uh_total_faixa_1,
    financiamento_pf_valor_total_faixa_1,
    financiamento_pf_uh_total_faixa_2,
    financiamento_pf_valor_total_faixa_2,
    financiamento_pf_uh_faixa_3_sem_fundo_social,
    financiamento_pf_valor_faixa_3_sem_fundo_social,
    financiamento_pf_uh_faixa_3_fundo_social,
    financiamento_pf_valor_faixa_3_fundo_social,
    financiamento_pf_uh_total_classe_media,
    financiamento_pf_valor_total_classe_media,
    financiamento_pf_uh_pro_cotista_geral,
    financiamento_pf_valor_pro_cotista_geral,
    financiamento_pf_uh_pro_cotista_faixa_1,
    financiamento_pf_valor_pro_cotista_faixa_1,
    financiamento_pf_uh_pro_cotista_faixa_2,
    financiamento_pf_valor_pro_cotista_faixa_2,
    financiamento_pf_uh_pro_cotista_faixa_3,
    financiamento_pf_valor_pro_cotista_faixa_3,
    financiamento_pf_uh_pro_cotista_classe_media,
    financiamento_pf_valor_pro_cotista_classe_media,
    financiamento_pf_uh_fora_mcmv,
    financiamento_pf_valor_fora_mcmv,
    -- Campos calculados
    financiamento_pf_valor_total_faixa_1
        + financiamento_pf_valor_total_faixa_2
        + COALESCE(financiamento_pf_valor_faixa_3_sem_fundo_social, 0)
        + COALESCE(financiamento_pf_valor_faixa_3_fundo_social, 0)
        + COALESCE(financiamento_pf_valor_total_classe_media, 0)
        + COALESCE(financiamento_pf_valor_fora_mcmv, 0)            AS valor_total_calculado
FROM {{ source('conjuntura_bronze', 'bronze_fgts_financiamentos_habitacionais') }}