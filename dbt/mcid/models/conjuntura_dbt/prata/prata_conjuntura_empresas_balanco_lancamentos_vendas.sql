{{ config(materialized='table') }}

-- Prata do conjuntura: balanços das construtoras (dado MANUAL).
-- Diferente das demais silvers (que leem o parquet da staging via pg_duckdb),
-- este dado foi inserido direto no Postgres (schema `empresas`), então a prata
-- lê a tabela nativa. Já vem tipado, então é passthrough.
-- Empresas: MRV, Cury, Tenda, Direcional, Pacaembu, Plano & Plano.

select *
from {{ source('conjuntura_manual', 'bronze_manual_empresas_balanco_lancamentos_vendas') }}
