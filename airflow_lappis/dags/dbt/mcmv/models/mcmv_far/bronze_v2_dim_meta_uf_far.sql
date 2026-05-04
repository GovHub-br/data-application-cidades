{{ config(materialized='table') }}

-- Tabela placeholder. A fonte oficial das metas por UF ainda está como "Decisão Pendente" (vide markdown).
-- Quando houver a definição, esta tabela poderá ser substituída por um seed (CSV) ou outra fonte real.
SELECT
    CAST(NULL AS INTEGER) AS ano,
    CAST(NULL AS VARCHAR) AS no_uf,
    CAST(NULL AS INTEGER) AS meta_uh,
    CAST(NULL AS NUMERIC) AS meta_valor,
    CAST(NULL AS VARCHAR) AS fonte_portaria,
    CAST(NULL AS DATE) AS dt_inicio_vigencia,
    CAST(NULL AS DATE) AS dt_fim_vigencia
WHERE False
