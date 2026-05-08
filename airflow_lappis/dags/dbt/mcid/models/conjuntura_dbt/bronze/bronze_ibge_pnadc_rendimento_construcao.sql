{{ config(materialized='table') }}

SELECT
    CAST(variavel_id        AS INT)       AS variavel_id,
    CAST(variavel_nome      AS TEXT)      AS variavel_nome,
    CAST(localidade_id      AS INT)       AS localidade_id,
    CAST(localidade_nome    AS TEXT)      AS localidade_nome,
    CAST(classificacao_id   AS TEXT)      AS classificacao_id,
    CAST(classificacao_nome AS TEXT)      AS classificacao_nome,
    CAST(categoria_id       AS TEXT)      AS categoria_id,
    CAST(categoria_nome     AS TEXT)      AS categoria_nome,
    CAST(unidade            AS TEXT)      AS unidade,
    CAST(periodo            AS TEXT)      AS periodo,
    TO_DATE(periodo, 'YYYYMM')           AS data_referencia,
    CAST(valor              AS NUMERIC)   AS valor,
    CAST(dt_ingest          AS TIMESTAMP) AS dt_ingest
FROM {{ source('ibge', 'pnadc_rendimento_construcao') }}