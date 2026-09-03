{{ config(materialized='table') }}

-- Silver do conjuntura contínuo: Saldo/poupança SBPE (ABECIP).
-- pg_duckdb lê o parquet tipado da staging (MinIO) direto do Postgres.
-- Parquet já sai tipado da ingestão (Etapa 02), então a silver é passthrough.
-- Full-refresh: cada run reconstrói a tabela a partir do parquet atual.

select
    data_referencia::date as data_referencia,
    deposito::numeric as deposito,
    retirada::numeric as retirada,
    captacao_liquida_valor::numeric as captacao_liquida_valor,
    captacao_liquida_pct::numeric as captacao_liquida_pct,
    saldo::numeric as saldo
from {{ ref('bnz_abecip_poupanca_sbpe') }}
