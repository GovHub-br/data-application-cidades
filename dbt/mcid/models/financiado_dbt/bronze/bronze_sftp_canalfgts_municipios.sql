{{ config(materialized="table") }}

-- Bronze: Domínio: municípios, com o código CAIXA e o código IBGE lado a lado.
-- Fonte: SFTP — sftp/caixa.geavo/GEAVO/ (pacote semanal `MC<aaaammdd>.zip`)
{% set padrao = "s3://data-lake-mcid/staging/**/*__MCidades_AO_1__municipios.parquet" %}

select *
from read_parquet('{{ padrao }}', filename => true, union_by_name => true) as r
where cast(r['filename'] as varchar) = {{ arquivo_mais_recente(padrao) }}
