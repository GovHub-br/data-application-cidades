{{ config(materialized='table') }}

SELECT
    data_referencia::date                                                   AS data_referencia,
    imoveis_residenciais_locacao_var_mensal_total::numeric                  AS var_mensal,
    imoveis_residenciais_locacao_var_ano_total::numeric                     AS var_ano,
    ROUND(
        ((1 + imoveis_residenciais_locacao_var_mensal_total::numeric)
        / NULLIF(1 + LAG(imoveis_residenciais_locacao_var_mensal_total::numeric, 12)
            OVER (ORDER BY data_referencia::date), 0) - 1) * 100,
        2
    )                                                                       AS var_12_meses_calc,
    {{ add_metadata_timestamps('silver') }}
FROM {{ ref('bronze_fipezap_locacao') }}