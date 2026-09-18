{{ config(materialized="table") }}

-- Silver do conjuntura contínuo: Financiamentos imobiliários PF/PJ (BACEN SGS).
-- pg_duckdb lê o parquet tipado da staging (MinIO) direto do Postgres.
-- Parquet já sai tipado da ingestão (Etapa 02), então a silver é passthrough.
-- Full-refresh: cada run reconstrói a tabela a partir do parquet atual.
select *
from read_parquet('s3://data-lake-mcid/staging/bacen/financiamentos_imobiliarios.parquet')
