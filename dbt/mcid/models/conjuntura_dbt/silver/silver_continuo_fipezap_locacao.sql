{{ config(materialized='table') }}

-- Silver do conjuntura contínuo: Índice FipeZap de locação (FIPE).
-- pg_duckdb lê o parquet tipado da staging (MinIO) direto do Postgres.
-- Parquet já sai tipado da ingestão (Etapa 02), então a silver é passthrough.
-- Full-refresh: cada run reconstrói a tabela a partir do parquet atual.

select
    data_referencia::date as data_referencia,
    imoveis_residenciais_locacao_numero_indice_total::numeric as imoveis_residenciais_locacao_numero_indice_total,
    imoveis_residenciais_locacao_var_mensal_total::numeric as imoveis_residenciais_locacao_var_mensal_total,
    imoveis_residenciais_locacao_var_ano_total::numeric as imoveis_residenciais_locacao_var_ano_total,
    dt_ingest::timestamp as dt_ingest
from {{ ref('bronze_continuo_fipezap_locacao') }}
