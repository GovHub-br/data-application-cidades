{{ config(materialized="table") }}

-- Silver do conjuntura contínuo: empregos na construção de edifícios (Novo CAGED).
-- Página 3, seção 4 (Empregos): criação líquida (saldo) e total de postos (estoque).
-- Lê o parquet tipado da staging (pg_duckdb). Full-refresh.
-- Colunas: ano, mes, admitidos, desligados, saldo, estoque, variacao, dt_ingest.
select *
from
    read_parquet(
        's3://data-lake-mcid/staging/novo_caged/saldo_estoque_construcao_edificios.parquet'
    )
