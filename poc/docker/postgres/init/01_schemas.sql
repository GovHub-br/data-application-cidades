-- Schemas precisam PREEXISTIR: o dbt-duckdb não emite CREATE SCHEMA em catálogos
-- anexados que não sejam DuckDB, então o model silver falharia com "Schema not found".
CREATE EXTENSION IF NOT EXISTS pg_duckdb;

CREATE SCHEMA IF NOT EXISTS silver;        -- destino dos models silver (artefato E)
CREATE SCHEMA IF NOT EXISTS sftp;          -- réplica do fluxo de hoje (artefato D)
CREATE SCHEMA IF NOT EXISTS bronze_view;   -- Opção 2: view pg_duckdb sobre o parquet tipado
