{{ config(materialized='table') }}

-- Silver do conjuntura contínuo: OGU: dotação/execução MCID (SIAFI/Tesouro).
-- pg_duckdb lê o parquet tipado da staging (MinIO) direto do Postgres.
-- Parquet já sai tipado da ingestão (Etapa 02), então a silver é passthrough.
-- Full-refresh: cada run reconstrói a tabela a partir do parquet atual.

select *
from read_parquet('s3://data-lake-mcid/staging/siafi-tesouro-gerencial/dotacao_execucao_outras_fontes_mcid.parquet')
