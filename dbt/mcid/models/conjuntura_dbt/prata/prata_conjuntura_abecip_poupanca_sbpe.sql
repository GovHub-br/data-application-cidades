{{ config(materialized='table') }}

-- Prata do conjuntura: Saldo/poupança SBPE (ABECIP).
-- pg_duckdb lê o parquet tipado da staging (MinIO) direto do Postgres.
-- Parquet já sai tipado da ingestão (Etapa 02), então a prata é passthrough.
-- Full-refresh: cada run reconstrói a tabela a partir do parquet atual.
--
-- Ingestão nova (plugins/ingestion, 10/2026): a staging guarda a aba
-- `SBPE_Mensal` como veio, com o cabeçalho da linha 5 (`Período`, `Depósito`…; a
-- coluna do % da captação não tem nome e vira `column_5`). Ficam só as linhas
-- mensais: saem os totais anuais (`Total.AAAA`), a linha de unidades e os meses
-- futuros, que a ABECIP já deixa na planilha com captação 0 e saldo vazio.
-- `rendimento` entra para o teste `conjuntura_abecip_poupanca_identidades`.

with mensal as (
    select *
    from {{ ref('bronze_abecip_poupanca_sbpe') }}
    where "Período" like '____-__-__%'
)

select
    cast("Período" as timestamp)::date        as data_referencia,
    "Depósito"::numeric                       as deposito,
    "Retirada"::numeric                       as retirada,
    "Captação líquida"::numeric               as captacao_liquida_valor,
    column_5::numeric                         as captacao_liquida_pct,
    "Rendimento"::numeric                     as rendimento,
    "Saldo"::numeric                          as saldo,
    {{ lake_dt_ingest() }}                    as dt_ingest
from mensal
where not ("Captação líquida"::numeric = 0 and "Saldo" is null)
