{{ config(materialized="table") }}

SELECT
    CAST(data_referencia                                AS DATE)      AS data_referencia,
    CAST(imoveis_residenciais_locacao_var_mensal_total  AS NUMERIC)   AS imoveis_residenciais_locacao_var_mensal_total,
    CAST(imoveis_residenciais_locacao_var_ano_total     AS NUMERIC)   AS imoveis_residenciais_locacao_var_ano_total,
    CAST(fonte                                          AS VARCHAR)   AS fonte,
    CAST(dt_ingest                                      AS TIMESTAMP) AS dt_ingest
FROM {{ source('fipe', 'indice_locacao') }}
