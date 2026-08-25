{{ config(materialized='table') }}

-- Silver do conjuntura contínuo: empregos no total da construção (todas as
-- divisões CNAE, Novo CAGED). Página 3, seção 4 (Empregos) — substitui a
-- coluna "Total" que antes vinha do manual (dados_mensais/boletim.xlsx).
-- Lê o parquet tipado da staging (pg_duckdb). Full-refresh.
-- Colunas: ano, mes, admitidos, desligados, saldo, estoque, variacao, dt_ingest.

select *
from read_parquet('s3://data-lake-mcid/staging/novo_caged/saldo_estoque_total_construcao.parquet')
