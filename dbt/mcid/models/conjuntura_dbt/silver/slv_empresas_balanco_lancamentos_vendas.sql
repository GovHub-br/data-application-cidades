{{ config(materialized='table') }}

-- Silver do conjuntura contínuo: balanços das construtoras (dado MANUAL).
-- Diferente das demais silvers (que leem o parquet da staging via pg_duckdb),
-- este dado foi inserido direto no Postgres (schema `empresas`), então a silver
-- lê a tabela nativa. Já vem tipado, então é passthrough.
-- Empresas: MRV, Cury, Tenda, Direcional, Pacaembu, Plano & Plano.

select *
from conjuntura.bnz_manual_empresas_balanco_lancamentos_vendas
